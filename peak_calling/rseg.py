from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from task import Task, luigi, GzipOutputFile

import requests
import logging

class UnfinishedGenomeSections(Task):

    genome_version = luigi.Parameter()

    @property
    def parameters(self):
        return [self.genome_version]

    @property
    def _extension(self):
        return 'bed.gz'

    def output(self):
        super_output = super(UnfinishedGenomeSections, self).output()
        return GzipOutputFile(super_output.path)

    def run(self):
        logger = logging.getLogger('UnfinishedGenomeSections')
        # Download the data from the ucsc table browser
        data = {'jsh_pageVertPos': '0',
                # TODO: change clade and org for mice if needed
                'clade': 'mammal',
                'org': 'Human',
                'db': self.genome_version,
                'hgta_group': 'map',
                'hgta_track': 'gap',
                'hgta_table': 'gap',
                'hgta_regionType': 'genome',
                'position': 'chrX:151073054-151383976',
                'hgta_outputType': 'primaryTable',
                'boolshad.sendToGalaxy': '0',
                'boolshad.sendToGreat': '0',
                'boolshad.sendToGenomeSpace': '0',
                'hgta_outFileName': '',
                'hgta_compressType': 'none',
                'hgta_doTopSubmit': 'get+output'
                }

        response = requests.post('http://genome.ucsc.edu/cgi-bin/hgTables', params=data)
        data = response.text.split('\n')
        with self.output().open('w') as output:
            for line in data:
                if line.startswith('#') or not line.strip():
                    continue
                split_row = line.split('\t')
                try:
                    line_to_output = '\t'.join([split_row[1], split_row[2], split_row[3]])
                except IndexError:
                    logger.debug(line)
                    raise
                line_to_output += '\n'
                output.write(line_to_output)

if __name__ == '__main__':
    logging.getLogger('UnfinishedGenomeSections').setLevel(logging.DEBUG)
    logging.basicConfig()
    luigi.run()
