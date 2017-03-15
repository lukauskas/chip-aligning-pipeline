from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import re
import luigi
import requests
from chipalign.core.file_formats.yaml_file import YamlFile
from chipalign.core.task import Task

class SignalTracksList(Task):
    """
    Obtains a list of signal tracks that are available to download for a given cell type.

    :param cell_type: cell type to use
    :param genome_version: genome version
    """

    cell_type = luigi.Parameter()
    genome_version = luigi.Parameter()

    @property
    def parameters(self):
        return [self.cell_type, self.genome_version]

    def url(self):
        if self.genome_version == 'hg19':
            return 'http://egg2.wustl.edu/roadmap/data/byFileType/signal/consolidated/macs2signal/pval/'
        else:
            raise ValueError('Unsupported genome version {!r}'.format(self.genome_version))

    def output(self):
        return YamlFile(super(SignalTracksList, self).output().path)

    @property
    def _extension(self):
        return 'yml'

    def _run(self):
        url = self.url()
        response = requests.get(url)
        response.raise_for_status()

        hrefs = re.findall('href="([^"]+)"', response.text)
        bigwigs = filter(lambda x: x.endswith('bigwig'), hrefs)

        urls_for_cell_type = {}

        for filename in bigwigs:
            match = re.match('(?P<cell_type>\w+)-(?P<track>.*?).pval.signal.bigwig', filename)
            if match is None:
                raise Exception('Could not parse {!r}'.format(filename))

            if match.group('cell_type') == self.cell_type:
                full_url = os.path.join(url, filename)
                urls_for_cell_type[match.group('track')] = full_url

        if not urls_for_cell_type:
            raise Exception('No URLs for cell type {!r} have been recovered'.format(self.cell_type))

        self.output().dump(urls_for_cell_type)
