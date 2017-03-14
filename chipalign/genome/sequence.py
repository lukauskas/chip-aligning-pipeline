from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from chipalign.core.downloader import fetch
from chipalign.core.task import Task, luigi


class GenomeSequence(Task):
    """
    Downloads whole sequences of genomes from UCSC

    :param genome_version: genome version to use
    """

    _DOWNLOAD_URIS = {
        'hg18': 'http://hgdownload.cse.ucsc.edu/goldenPath/hg18/bigZips/hg18.2bit',
        'hg19': 'http://hgdownload.cse.ucsc.edu/goldenPath/hg19/bigZips/hg19.2bit',
        'hg38': 'http://hgdownload.cse.ucsc.edu/goldenPath/hg38/bigZips/hg38.2bit',
        'dm6': 'http://hgdownload.cse.ucsc.edu/goldenPath/dm6/bigZips/dm6.2bit',
        'sacCer3': 'http://hgdownload.cse.ucsc.edu/goldenPath/sacCer3/bigZips/sacCer3.2bit'
    }

    genome_version = luigi.Parameter()

    @property
    def parameters(self):
        return [self.genome_version]

    @property
    def _extension(self):
        return '2bit'

    def run(self):
        try:
            uri = self._DOWNLOAD_URIS[self.genome_version]
        except KeyError:
            raise ValueError('Unsupported genome version: {!r}'.format(self.genome_version))

        with self.output().open('wb') as output_file:
            fetch(uri, output_file)
