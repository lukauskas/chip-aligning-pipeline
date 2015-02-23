from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import shutil
from genome_browser import GenomeSequence
from task import Task, luigi, GzipOutputFile

import requests
import logging
from util import temporary_directory


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

class Deadzones(Task):

    genome_version = luigi.Parameter()
    width_of_kmers = luigi.IntParameter()

    prefix_length = luigi.IntParameter(default=3, significant=False)
    verbose = luigi.BooleanParameter(default=True, significant=False)

    @property
    def _genome_sequence_task(self):
        return GenomeSequence(genome_version=self.genome_version)

    def requires(self):
        return [self._genome_sequence_task]

    @property
    def parameters(self):
        return [self.genome_version, 'k{}'.format(self.width_of_kmers)]

    @property
    def _extension(self):
        return 'bed'

    def run(self):
        from command_line_applications.rseg import deadzones
        from command_line_applications.ucsc_suite import twoBitToFa

        logger = logging.getLogger('Deadzones')
        output_abspath = os.abspath(self.output().path)
        genome_sequence_abspath = os.abspath(self._genome_sequence_task.output().path)
        with temporary_directory(prefix='tmp-deadzones-',
                                 cleanup_on_exception=False, logger=logger):
            fa_sequence_filename = 'sequence.fa'
            logger.debug('Converting sequence to fasta')
            twoBitToFa(genome_sequence_abspath, fa_sequence_filename)

            deadzones_output_file = 'deadzones.bed'

            deadzones_args = ['-p', self.prefix_length,
                              '-s', 'fa',
                              '-k', self.width_of_kmers,
                              '-o', deadzones_output_file,
                              ]
            if self.verbose:
                deadzones_args.append('-v')
            logger.debug('Computing deadzones (takes ages :( )')
            deadzones(*deadzones_args)
            logger.debug('Deadzones done, moving file to output location')
            shutil.move(deadzones_output_file, output_abspath)


if __name__ == '__main__':
    logging.getLogger('UnfinishedGenomeSections').setLevel(logging.DEBUG)
    logging.getLogger('Deadzones').setLevel(logging.DEBUG)
    logging.basicConfig()
    luigi.run()
