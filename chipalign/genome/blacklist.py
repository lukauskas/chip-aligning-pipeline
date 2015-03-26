from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from itertools import imap
import logging
import os
import gzip
import shutil
import pybedtools
import luigi

from chipalign.core.downloader import fetch
from chipalign.core.task import Task
from chipalign.core.util import temporary_directory

class BlacklistedRegions(Task):

    DOWNLOADABLE_BLACKLISTS = {'hg19': 'http://hgdownload.cse.ucsc.edu/goldenPath/hg19/encodeDCC/wgEncodeMapability/wgEncodeDacMapabilityConsensusExcludable.bed.gz'}

    CHAIN_FILES = {
        ('hg19', 'hg18'): 'http://hgdownload.soe.ucsc.edu/goldenPath/hg19/liftOver/hg19ToHg18.over.chain.gz',
        ('hg19', 'hg38'): 'http://hgdownload.soe.ucsc.edu/goldenPath/hg19/liftOver/hg19ToHg38.over.chain.gz'}

    genome_version = luigi.Parameter()

    def requires(self):
        if self.genome_version in self.DOWNLOADABLE_BLACKLISTS:
            return []
        else:
            return BlacklistedRegions(genome_version=self.DOWNLOADABLE_BLACKLISTS.keys()[0])

    @property
    def _extension(self):
        return 'bed.gz'

    @property
    def parameters(self):
        return [self.genome_version]


    def run(self):
        logger = self.logger()

        if self.genome_version in self.DOWNLOADABLE_BLACKLISTS:
            url = self.DOWNLOADABLE_BLACKLISTS[self.genome_version]
            logger.debug('Downloading the blacklist directly from {}'.format(url))
            output_abspath = os.path.abspath(self.output().path)
            with self.temporary_directory():
                tmp_file = 'download.gz'

                with open(tmp_file, 'w') as f:
                    fetch(url, f)
                shutil.move(tmp_file, output_abspath)
        else:
            from chipalign.command_line_applications import crossmap

            input_genome_version = self.requires().genome_version
            input_abspath = os.path.abspath(self.input().path)
            output_abspath = os.path.abspath(self.output().path)
            with temporary_directory(logger=logger, prefix='tmp-blacklistedregions'):
                chain_filename = 'chain_file.chain.gz'
                logger.debug('Downloading chain')
                with open(chain_filename, 'w') as chain_out:
                    fetch(self.CHAIN_FILES[(input_genome_version, self.genome_version)], chain_out)

                logger.debug('Lifting coordinates')
                tmp_blacklist_filename = 'blacklist.bed'
                crossmap('bed', chain_filename, input_abspath, tmp_blacklist_filename)

                logger.debug('Gzipping')
                tmp_gzip_file = 'blacklist.bed.gz'
                with gzip.GzipFile(tmp_gzip_file, 'w') as out_:
                    with open(tmp_blacklist_filename, 'r') as in_:
                        out_.writelines(in_)

                logger.debug('Moving {} to {}'.format(tmp_gzip_file, output_abspath))
                shutil.move(tmp_gzip_file, output_abspath)

class NonBlacklisted(Task):
    genome_version = BlacklistedRegions.genome_version

    input_task = luigi.Parameter()

    @property
    def parameters(self):
        return [self.input_task.task_class_friendly_name] + self.input_task.parameters + [self.genome_version]

    @property
    def _blacklist_task(self):
        return BlacklistedRegions(genome_version=self.genome_version)

    def requires(self):
        return [self.input_task, self._blacklist_task]

    @property
    def _extension(self):
        return 'bed.gz'

    def run(self):

        blacklist = pybedtools.BedTool(self._blacklist_task.output().path)
        input_ = pybedtools.BedTool(self.input_task.output().path)

        difference = input_.intersect(blacklist, v=True)
        try:
            with self.output().open('w') as f:
                f.writelines(imap(str, difference))
        finally:
            difference_fn = difference.fn
            del difference
            try:
                os.unlink(difference_fn)
            except OSError:
                if os.path.isfile(difference_fn):
                    raise




