from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from six.moves import map as imap

import os
import tarfile
import shutil
import tempfile
import logging

import luigi
import numpy as np
from chipalign.core.file_formats.dataframe import DataFrameFile
from chipalign.core.util import fast_bedtool_from_iterable, timed_segment, autocleaning_pybedtools, \
    temporary_file

from chipalign.core.task import Task
from chipalign.core.downloader import fetch
from chipalign.core.file_formats.file import File
import pandas as pd

def drop_unmappable(reads_bedtool, mappability_bedtool):
    return reads_bedtool.intersect(mappability_bedtool, f=1)

class GenomeMappabilityTrack(Task):
    """
    Downloads genome mabpabbility track for a particular read length

    Since Kundaje haven't updated his versions, we need to download one from hoffman lab:
    https://bismap.hoffmanlab.org/

    Hoffman has completely redesigned the format of this so we will have to adapt our scripts.

    :param genome_version:
    :param read_length:
    """
    genome_version = luigi.Parameter()
    read_length = luigi.IntParameter()

    @property
    def _track_uri(self):

        if self.genome_version in ['hg19', 'hg38', 'mm9', 'mm10'] \
                and self.read_length in [24, 36, 50, 100]:
            return 'https://bismap.hoffmanlab.org/raw/{}/k{}.umap.bed.gz'.format(self.genome_version,
                                                                                 self.read_length)
        else:
            raise Exception('Mappability track unsupported for {} k{}'.format(
                self.genome_version, self.read_length))

    @property
    def parameters(self):
        return [self.genome_version, 'k{}'.format(self.read_length)]

    @property
    def _extension(self):
        return 'bed.gz'

    def _run(self):
        from chipalign.command_line_applications.archiving import gunzip

        logger = self.logger()

        self.ensure_output_directory_exists()
        output_abspath = os.path.abspath(self.output().path)

        # So Hoffman provides tracks as bed files these days.
        # Entry in a bed file implies that the region is mappable

        with self.temporary_directory():
            uri = self._track_uri

            logger.debug('Fetching the mappability data from {}'.format(uri))
            track_file = 'umap.bed.gz'

            with open(track_file, 'wb') as buffer:
                fetch(uri, buffer)

            # Don't know why Hoffman's output contains intervals that are not merged, but
            # Let's make our life simple and merge them

            # Pybedtools is funny and doesn't work otherwise go figure
            gunzip(track_file)
            # Remove ".gz"
            track_file = track_file[:-3]

            merged_output_file = 'umap.merged.bed.gz'

            with autocleaning_pybedtools() as pybedtools:
                bd = pybedtools.BedTool(track_file)
                merged = bd.merge()
                merged.saveas(merged_output_file)

            shutil.move(merged_output_file, output_abspath)


class FullyMappableBins(Task):
    """
    Returns only the bins in `bins_task` that are fully mappable.

    See also :class:`~chipalign.genome.mappability.BinMappability` task.

    :param bins_task: bins task to use
    :param read_length: the length of reads that are being mapped
    :param max_ext_size: maximum size of read extension that is used in MACS
    """
    bins_task = luigi.Parameter()
    read_length = GenomeMappabilityTrack.read_length
    max_ext_size = luigi.IntParameter()

    @property
    def task_class_friendly_name(self):
        return 'FMB'

    @property
    def genome_version(self):
        return self.bins_task.genome_version

    @property
    def mappability_track(self):
        return GenomeMappabilityTrack(genome_version=self.genome_version,
                                      read_length=self.read_length)

    def requires(self):
        return [self.bins_task, self.mappability_track]

    @property
    def _extension(self):
        return 'bed.gz'

    @property
    def parameters(self):
        return self.bins_task.parameters + ['k{}'.format(self.read_length), 'e{}'.format(self.max_ext_size)]

    def _run(self):
        logger = self.logger()

        with autocleaning_pybedtools() as pybedtools:
            bins = pybedtools.BedTool(self.bins_task.output().path)

            len_before = len(bins)
            mappability = pybedtools.BedTool(self.mappability_track.output().path)

            # Shorten the size of mappability track by max_ext_length from both sides
            slop_amount = self.max_ext_size
            # Remove regions that are shorter than shrink amount
            mappability = mappability.filter(lambda x: x.length > slop_amount*2)
            # Shrink the remaining regions
            mappability = mappability.slop(b=-slop_amount, genome=self.genome_version)

            filtered_bins = drop_unmappable(bins, mappability)

            len_after = len(filtered_bins)
            difference = len_before-len_after

            logger.debug('Removed {:,} ({:.2%}) as they are unmappable'.format(difference,
                                                                               difference/len_before))
            with temporary_file() as tf:
                filtered_bins.saveas(tf, compressed=True)
                shutil.move(tf, self.output().path)
