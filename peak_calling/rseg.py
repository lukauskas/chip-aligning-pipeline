from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import shutil
import gzip
from genome_browser import GenomeSequence
from task import Task, luigi, GzipOutputFile

import requests
import logging
from util import temporary_directory


class UnfinishedGenomeSections(Task):

    genome_version = luigi.Parameter()

    _GENOME_CLASIFICATIONS = {'hg': {'clade': 'mammal',
                                     'org': 'Human'},
                              'dm': {'clade': 'insect',
                                     'org': 'D. melanogaster'},
                              'mm': {'clade': 'mammal',
                                     'org': 'Mouse'}}

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
        data.update(self._GENOME_CLASIFICATIONS[self.genome_version[:2]])
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

class UnmappableRegions(Task):

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

        logger = logging.getLogger('UnmappableRegions')
        output_abspath = os.path.abspath(self.output().path)
        genome_sequence_abspath = os.path.abspath(self._genome_sequence_task.output().path)
        with temporary_directory(prefix='tmp-unmappable-',
                                 cleanup_on_exception=False, logger=logger):
            fa_sequence_filename = 'sequence.fa'
            logger.debug('Converting sequence to fasta')
            twoBitToFa(genome_sequence_abspath, fa_sequence_filename)

            deadzones_output_file = 'deadzones.bed'

            deadzones_args = ['-p', self.prefix_length,
                              '-s', 'fa',
                              '-k', self.width_of_kmers,
                              '-o', deadzones_output_file,
                              fa_sequence_filename,
                              ]
            if self.verbose:
                deadzones_args.append('-v')
            logger.debug('Computing deadzones (takes ages :( )')
            deadzones(*deadzones_args)
            logger.debug('Deadzones done, moving file to output location')
            shutil.move(deadzones_output_file, output_abspath)

class Deadzones(Task):

    genome_version = UnmappableRegions.genome_version
    width_of_kmers = UnmappableRegions.genome_version
    prefix_length = UnmappableRegions.prefix_length

    @property
    def unmappable_regions_task(self):
        return UnmappableRegions(genome_version=self.genome_version,
                                 width_of_kmers=self.width_of_kmers,
                                 prefix_length=self.prefix_length)

    @property
    def unfinished_genome_sections_task(self):
        return UnfinishedGenomeSections(genome_version=self.genome_version)

    def requires(self):
        return [self.unmappable_regions_task, self.unfinished_genome_sections_task]

    @property
    def _extension(self):
        return 'bed.gz'

    def output(self):
        super_output = super(Deadzones, self).output()
        return GzipOutputFile(super_output.path)

    def run(self):
        logger = logging.getLogger('Deadzones')
        from command_line_applications.common import sort, cat, cut
        from command_line_applications.rseg import rseg_join

        abspath_unmappable = os.path.abspath(self.unmappable_regions_task.output().path)
        abspath_unfinished = os.path.abspath(self.unfinished_genome_sections_task.output().path)

        with temporary_directory(prefix='tmp-deadzones-', logger=logger, cleanup_on_exception=False):
            unfinished_ungzipped_filename = 'unfinished.bed'
            with gzip.GzipFile(abspath_unfinished, 'r') as in_:
                with open(unfinished_ungzipped_filename, 'w') as out:
                    out.writelines(in_)

            logger.debug('Sorting and joining the data')
            joined_data = rseg_join(sort(cat(unfinished_ungzipped_filename, abspath_unmappable, _piped=True),
                                    '-k 1,1', '-k2,2n', _piped=True),
                                    _iter=True)

            with self.output().open('w') as f:
                f.writelines(joined_data)


if __name__ == '__main__':
    logging.getLogger('UnfinishedGenomeSections').setLevel(logging.DEBUG)
    logging.getLogger('UnmappableRegions').setLevel(logging.DEBUG)
    logging.getLogger('Deadzones').setLevel(logging.DEBUG)
    logging.basicConfig()
    luigi.run()
