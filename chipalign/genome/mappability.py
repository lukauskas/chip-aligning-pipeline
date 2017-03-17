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


class MappabilityTrack(object):
    """
    MappabilityTrack object
    """

    __lookup_dict = None

    def __init__(self, lookup_dict):
        self.__lookup_dict = lookup_dict

    def filter_uniquely_mappables(self, bedtool_df):
        logger = logging.getLogger(self.__class__.__name__)
        chromosomes = bedtool_df.chrom.unique()
        chromosomes_in_mappability_track = set(self.__lookup_dict.keys())

        chromosomes_not_in_mappability_track = set(chromosomes) - chromosomes_in_mappability_track
        if chromosomes_not_in_mappability_track:
            raise Exception('No mappability track for chromosomes {}.'.format(
                sorted(chromosomes_not_in_mappability_track)))

        answer = []
        for chromosome, chromosome_df in bedtool_df.groupby('chrom'):

            chromosome_lookup = self.__lookup_dict[chromosome]

            # Now the start coordinate is the one we need to check, irregardless of strand
            # this is how one should interpret the README
            # http://egg2.wustl.edu/roadmap/data/byFileType/mappability/README
            unique = chromosome_lookup[chromosome_df.start]
            chromosome_df = chromosome_df[unique]

            answer.append(chromosome_df)

        return pd.concat(answer)

    @classmethod
    def maximum_mappability_score_for_bin(cls, bin_width, read_length,
                                          extension_length):
        # see number_of_uniquely_mappable_within_a_bin below
        # [bin_.start + bin_.width] - (bin_.start - (extension_length + read_length - 1))
        # +
        # [bin_.start + bin_.width] + extension_length - (bin_.start - (read_length - 1))
        # ------------------------------------------------------------------------------
        # bin_.width + extension_length + read_length - 1
        # +
        # bin_.width + extension_length + read_length - 1
        return 2 * (bin_width + extension_length + read_length - 1)

    def number_of_uniquely_mappable_within_a_bin(self, bins_bed_df, read_length,
                                                 extension_length):
        logger = logging.getLogger(self.__class__.__name__)

        answer = []
        skipped_chroms = set()
        for chromosome, bins_chromosome in bins_bed_df.groupby('chrom'):

            try:
                chromosome_lookup = self.__lookup_dict[chromosome]
            except KeyError:
                skipped_chroms.add(chromosome)
                continue

            chromosome_length = len(chromosome_lookup)

            # see comment below on why we want to prepend zero
            cum_lookup = np.concatenate([[0], np.cumsum(chromosome_lookup)])

            bins_chromosome = bins_chromosome.copy()

            # Positive strand

            min_anchor_location = bins_chromosome.start - (
                extension_length + read_length - 1)
            max_anchor_location = bins_chromosome.end

            min_anchor_location = min_anchor_location.clip(0, chromosome_length)
            max_anchor_location = max_anchor_location.clip(0, chromosome_length)

            # We want to compute np.sum(chromosome_lookup[min_anchor_location:max_anchor_location])
            # this is faster to do by using cumulative sums as it is equivalent to
            # cum_lookup[max_anchor_location] - cum_lookup[min_anchor_location]
            # Note that cum_lookup has zero at the front prepended to it (otherwise the equations
            # would have -1 in them, and there would be a special case to deal when loc is 0

            uniquely_mappable_per_bin = cum_lookup[max_anchor_location] - cum_lookup[min_anchor_location]

            # Negative strand
            min_anchor_location = bins_chromosome.start - (read_length - 1)
            max_anchor_location = bins_chromosome.end + extension_length

            min_anchor_location = min_anchor_location.clip(0, chromosome_length)
            max_anchor_location = max_anchor_location.clip(0, chromosome_length)

            uniquely_mappable_per_bin += cum_lookup[max_anchor_location] - cum_lookup[
                min_anchor_location]

            bins_chromosome['mappability'] = uniquely_mappable_per_bin
            answer.append(bins_chromosome)

        answer = pd.concat(answer)

        logger.warn('No mappability data for chromosomes {!r}. '
                    'Assuming unmappable.'.format(sorted(skipped_chroms)))

        answer.sort_values(by=['chrom', 'start', 'end'], inplace=True)
        answer = answer.set_index(['chrom', 'start', 'end'])['mappability']

        return answer

    def is_uniquely_mappable(self, chromosome, start, end, strand):
        chromosome_lookup = self.__lookup_dict[chromosome]

        # Now the start coordinate is the one we need to check, irregardless of strand
        # this is how one should interpret the README
        # http://egg2.wustl.edu/roadmap/data/byFileType/mappability/README
        anchor_locus = int(start)

        return chromosome_lookup[anchor_locus]


class MappabilityInfoFile(File):
    def dump(self, data, verify=True):
        logger = logging.getLogger('MappabilityInfoFile.dump')

        __, tmp_location_for_archive = tempfile.mkstemp(suffix='.npz')

        try:
            logger.debug('Dumping the data to npz archive')
            np.savez_compressed(tmp_location_for_archive, **data)

            if verify:
                logger.debug('Verifying data was written correctly')
                processed_tracks_loaded = np.load(tmp_location_for_archive)

                if sorted(processed_tracks_loaded.keys()) != sorted(
                        data.keys()):
                    raise IOError(
                        'Problem dumping tracks to archive. Keys don\'t match')

                for key in data.keys():
                    if not np.equal(data[key],
                                    processed_tracks_loaded[key]).all():
                        raise IOError(
                            'Problem dumping tracks to archive. Data for {} does not match'.format(
                                key))

            logger.debug('Moving file to correct location')
            shutil.move(tmp_location_for_archive, self.path)
        finally:
            try:
                os.unlink(tmp_location_for_archive)
            except OSError:
                if os.path.isfile(tmp_location_for_archive):
                    raise

    def load(self):
        data = np.load(self.path)
        return MappabilityTrack(data)


class GenomeMappabilityTrack(Task):
    """
    Downloads genome mabpabbility track for a particular read length

    :param genome_version:
    :param read_length:
    """
    genome_version = luigi.Parameter()
    read_length = luigi.IntParameter()

    @property
    def _track_uri(self):
        if self.genome_version == 'hg19':
            if 20 <= self.read_length <= 54:
                return 'http://egg2.wustl.edu/roadmap/data/byFileType/mappability/encodeHg19Male/globalmap_k20tok54.tgz'
            else:
                raise Exception('Read length: {} unsupported for {}'.format(
                    self.read_length, self.genome_version))
        else:
            raise Exception('Mappability track unsupported for {}'.format(
                self.genome_version))

    @property
    def parameters(self):
        return [self.genome_version, 'k{}'.format(self.read_length)]

    @property
    def _extension(self):
        return 'npz'

    def output(self):
        super_output = super(GenomeMappabilityTrack, self).output()
        return MappabilityInfoFile(super_output.path)

    def _run(self):
        logger = self.logger()

        self.ensure_output_directory_exists()

        processed_tracks = {}

        with self.temporary_directory():
            uri = self._track_uri

            logger.debug('Fetching the mappability data from {}'.format(uri))
            track_file = os.path.basename(uri)
            with open(track_file, 'wb') as buffer:
                fetch(uri, buffer)

            logger.debug('Processing {}'.format(track_file))

            with tarfile.open(track_file) as tar:
                for member in tar.getmembers():
                    if not member.isfile():
                        continue

                    name = os.path.basename(member.name)
                    chromosome, dtype_str, __ = name.split('.')

                    with timed_segment('Processing chromosome {}'.format(chromosome), logger):
                        assert chromosome not in processed_tracks  # Sanity checks

                        if dtype_str == 'uint8':
                            dtype = np.uint8
                        else:
                            raise Exception('Unknown dtype: {}'.format(dtype_str))

                        file_ = tar.extractfile(member)
                        mappability_track = np.frombuffer(file_.read(), dtype=dtype)

                        # straight from http://egg2.wustl.edu/roadmap/data/byFileType/mappability/README
                        mappability_track = (mappability_track > 0) & (
                        mappability_track <= self.read_length)

                        processed_tracks[chromosome] = mappability_track

        logger.debug('Saving output')
        self.output().dump(processed_tracks)


class BinMappability(Task):
    """
    For each of the provided bins, determines how many bins are mappable.

    :param bins_task: bins task to use
    :param read_length: the length of reads that are being mapped
    :param max_ext_size: maximum size of read extension that is used in MACS
    """

    bins_task = luigi.Parameter()

    read_length = GenomeMappabilityTrack.read_length
    max_ext_size = luigi.IntParameter()

    @property
    def mappability_track_task(self):
        return GenomeMappabilityTrack(genome_version=self.bins_task.genome_version,
                                      read_length=self.read_length)

    def requires(self):
        return [self.bins_task, self.mappability_track_task]

    @property
    def _extension(self):
        return 'pd'

    @property
    def _output_class(self):
        return DataFrameFile

    def best_total_score(self):
        return MappabilityTrack.maximum_mappability_score_for_bin(
            bin_width=self.bins_task.window_size,
            read_length=self.read_length,
            extension_length=self.max_ext_size)

    def _run(self):
        logger = self.logger()

        with autocleaning_pybedtools() as pybedtools:
            genomic_windows = pybedtools.BedTool(
                self.bins_task.output().path)
            genomic_windows_df = genomic_windows.to_dataframe()

        mappability = self.mappability_track_task.output().load()

        logger.debug('Computing mappability')
        number_of_uniquely_mappable_per_bin = mappability.number_of_uniquely_mappable_within_a_bin(
            genomic_windows_df,
            read_length=self.read_length,
            extension_length=self.max_ext_size)

        logger.debug('Writing output')
        self.output().dump(number_of_uniquely_mappable_per_bin)


class FullyMappableBins(Task):
    """
    Returns only the bins in `bins_task` that are fully mappable.

    See also :class:`~chipalign.genome.mappability.BinMappability` task.

    :param bins_task: bins task to use
    :param read_length: the length of reads that are being mapped
    :param max_ext_size: maximum size of read extension that is used in MACS
    """

    bins_task = BinMappability.bins_task
    read_length = BinMappability.read_length
    max_ext_size = BinMappability.max_ext_size

    @property
    def task_class_friendly_name(self):
        return 'FMB'

    @property
    def bin_mappability_task(self):
        return BinMappability(bins_task=self.bins_task,
                              read_length=self.read_length,
                              max_ext_size=self.max_ext_size)

    def requires(self):
        return self.bin_mappability_task

    @property
    def _extension(self):
        return 'bed.gz'

    @property
    def parameters(self):
        return self.bin_mappability_task.parameters

    def _run(self):
        logger = self.logger()
        max_score = self.bin_mappability_task.best_total_score()
        logger.debug('Maximum possible score is: {}'.format(max_score))

        mappability_scores = self.input().load()
        fully_mappable = mappability_scores[mappability_scores == max_score]
        logger.info(
            'Bins total: {:,}, bins fully mappable: {:,} ({:%})'.format(
                len(mappability_scores), len(fully_mappable),
                float(len(fully_mappable)) / len(mappability_scores)))
        fully_mappable = fully_mappable.reset_index()
        with temporary_file() as tf:
            fully_mappable.to_csv(tf, sep='\t', compression='gzip',
                                  columns=['chrom', 'start', 'end'])

            shutil.move(tf, self.output().path)
