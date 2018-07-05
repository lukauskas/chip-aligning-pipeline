from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import shutil
import os
import glob

import luigi

from chipalign.genome.sequence import GenomeSequence
from chipalign.core.task import Task
from chipalign.core.downloader import fetch


class BwaIndex(Task):

    """
    Downloads BWA index for the specified genome version

    :param genome_version: Genome version
    """
    genome_version = luigi.Parameter()

    # Script is designed for illumina's iGenome
    # https://support.illumina.com/sequencing/sequencing_software/igenome.html

    _DOWNLOADABLE_INDICES = {'hg18': 'ftp://igenome:G3nom3s4u@ussd-ftp.illumina.com/Homo_sapiens/UCSC/hg18/Homo_sapiens_UCSC_hg18.tar.gz',
                             'hg19': 'ftp://igenome:G3nom3s4u@ussd-ftp.illumina.com/Homo_sapiens/UCSC/hg19/Homo_sapiens_UCSC_hg19.tar.gz',
                             'hg38': 'ftp://igenome:G3nom3s4u@ussd-ftp.illumina.com/Homo_sapiens/UCSC/hg38/Homo_sapiens_UCSC_hg38.tar.gz',
                             'dm6': 'ftp://igenome:G3nom3s4u@ussd-ftp.illumina.com/Drosophila_melanogaster/UCSC/dm6/Drosophila_melanogaster_UCSC_dm6.tar.gz',
                             }

    _ARCHIVE_PATHS = {
        'dm6': 'Drosophila_melanogaster/UCSC/dm6/Sequence/BWAIndex/version0.6.0/',
        'hg18': 'Homo_sapiens/UCSC/hg18/Sequence/BWAIndex/version0.6.0/',
        'hg19': 'Homo_sapiens/UCSC/hg19/Sequence/BWAIndex/version0.6.0/',
        'hg38': 'Homo_sapiens/UCSC/hg38/Sequence/BWAIndex/version0.6.0/',
    }
    @property
    def parameters(self):
        return [self.genome_version]

    @property
    def _extension(self):
        return 'zip'

    @property
    def _genome_sequence_task(self):
        return GenomeSequence(genome_version=self.genome_version)

    def requires(self):
        if self.genome_version not in self._DOWNLOADABLE_INDICES:
            return self._genome_sequence_task

    def _url(self):
        return self._DOWNLOADABLE_INDICES[self.genome_version]

    def _run(self):
        from chipalign.command_line_applications.archiving import zip_, tar

        logger = self.logger()
        self.ensure_output_directory_exists()
        url = self._url()

        with self.temporary_directory():

            temp_download_location = 'temp.tar.gz'

            logger.info('Downloading BWA index')
            with open(temp_download_location, 'wb') as handle:
                fetch(url, handle)

            logger.info('Extracting BWA index')

            os.makedirs('index')

            tar('-C', 'index', '-xf', temp_download_location, '--strip=6',
                self._ARCHIVE_PATHS[self.genome_version])

            os.unlink(temp_download_location)
            os.unlink('index/genome.fa')

            logger.info('Repackaging index')
            repackaged_name = 'index.repackaged.zip'
            zip_('-j', repackaged_name, glob.glob('index/*'))

            shutil.move(repackaged_name, self.output().path)
