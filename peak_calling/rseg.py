from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import shutil
import gzip
from genome_browser import GenomeSequence
from peak_calling.base import PeaksBase
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

class RsegPeaks(PeaksBase):

    width_of_kmers = Deadzones.genome_version
    prefix_length = Deadzones.prefix_length

    number_of_iterations = luigi.IntParameter()

    @property
    def deadzones_task(self):
        return Deadzones(genome_version=self.genome_version,
                         width_of_kmers=self.width_of_kmers,
                         prefix_length=self.prefix_length)

    @property
    def parameters(self):
        parameters = super(PeaksBase, self).parameters
        parameters.append('k{}'.format(self.width_of_kmers))
        parameters.append('i{}'.format(self.number_of_iterations))
        return parameters

    def requires(self):
        requirements = super(RsegPeaks, self).requires()
        requirements.append(self.deadzones_task)
        return requirements

    def run(self):
        logger = logging.getLogger('RsegPeaks')
        from command_line_applications.bedtools import bamToBed
        from command_line_applications.rseg import rseg
        import pybedtools

        alignments_abspath = os.path.abspath(self.alignment_task.output().path)
        deadzones_abspath = os.path.abspath(self.deadzones_task.output().path)

        peaks_output, stdout_output = self.output()
        peaks_output_abspath = os.path.abspath(peaks_output.path)
        stdout_output_abspath = os.path.abspath(stdout_output.path)

        with temporary_directory(prefix='tmp-rseg-peaks', logger=logger):
            bed_alignments_file = 'alignments.bed'
            logger.debug('Dumping reads to {}'.format(bed_alignments_file))
            bamToBed('-i', alignments_abspath, _out=bed_alignments_file)

            deadzones_file = 'deadzones.bed'
            logger.debug('Gunzipping deadzones to {}'.format(deadzones_file))

            with gzip.GzipFile(deadzones_abspath, 'r') as in_:
                with open(deadzones_file, 'w') as out:
                    out.writelines(in_)

            chromsizes_file = 'chromsizes.bed'
            logger.debug('Getting chromosome sizes to {}'.format(chromsizes_file))
            pybedtools.chromsizes_to_file(self.genome_version, chromsizes_file)

            output_directory = 'output'
            os.makedirs(output_directory)
            logger.debug('Running RSEG')

            stdout_file = 'stdout'
            rseg('-c', chromsizes_file,
                 '-i', self.number_of_iterations,
                 '-o', output_directory,
                 '-v',
                 bed_alignments_file,
                 _out=stdout_file)

            logger.debug('Looking for domains file')
            domains_file = filter(lambda x: x.endswith('-domains.bed'), os.listdir(output_directory))[0]
            logger.debug('Found {}'.format(domains_file))

            logger.debug('Filtering the output for enriched regions')
            filtered_domains_file = 'filtered-domains.bed'
            with open(domains_file, 'r') as in_:
                with open(filtered_domains_file, 'w') as out:
                    for line in in_:
                        if line.contains('ENRICHED'):
                            out.write(line)

            logger.debug('Moving')
            shutil.move(stdout_file, stdout_output_abspath)
            shutil.move(filtered_domains_file, peaks_output_abspath)

            logger.debug('Done')

if __name__ == '__main__':
    logging.getLogger('UnfinishedGenomeSections').setLevel(logging.DEBUG)
    logging.getLogger('UnmappableRegions').setLevel(logging.DEBUG)
    logging.getLogger('Deadzones').setLevel(logging.DEBUG)
    logging.getLogger('RsegPeaks').setLevel(logging.DEBUG)
    logging.basicConfig()
    luigi.run()
