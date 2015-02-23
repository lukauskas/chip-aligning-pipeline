from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os

import luigi
from genome_windows import NonOverlappingWindows
from peak_calling import Peaks

import pybedtools
import logging
import gzip
from task import Task, GzipOutputFile

def _intersection_counts_to_wiggle(output_file_handle,
                                   intersection_with_counts_bed,
                                   name,
                                   description,
                                   window_size,
                                   binarise=False):
    output_file_handle.write('track type=wiggle_0 name="{0}" description="{1}"\n'.format(
        name,
        description
    ))
    previous_chromosome = None
    for row in intersection_with_counts_bed:
        if row.count == 0:
            continue

        if row.chrom != previous_chromosome:
            output_file_handle.write('variableStep chrom={0} span={1}\n'.format(row.chrom, window_size))
            previous_chromosome = row.chrom

        # Add +1 to start as wig locations are 1-based
        start = row.start + 1
        count = row.count
        if binarise:
            count = 1 if count > 0 else 0

        output_file_handle.write('{0}\t{1}\n'.format(start, count))

class Profile(Task):

    genome_version = Peaks.genome_version

    experiment_accession = Peaks.experiment_accession
    study_accession = Peaks.study_accession
    experiment_alias = Peaks.experiment_alias

    bowtie_seed = Peaks.bowtie_seed
    pretrim_reads = Peaks.pretrim_reads

    broad = Peaks.broad

    window_size = NonOverlappingWindows.window_size
    binary = luigi.BooleanParameter()

    @property
    def _peaks_task(self):
        return Peaks(genome_version=self.genome_version,
                     experiment_accession=self.experiment_accession,
                     study_accession=self.study_accession,
                     experiment_alias=self.experiment_alias,
                     bowtie_seed=self.bowtie_seed,
                     pretrim_reads=self.pretrim_reads,
                     broad=self.broad)

    @property
    def parameters(self):
        parameters = self._peaks_task.parameters
        parameters.append('w{}'.format(self.window_size))
        if self.binary:
            parameters.append('b')

        return parameters

    @property
    def _extension(self):
        return 'wig.gz'

    @property
    def _genome_windows_task(self):
        return NonOverlappingWindows(genome_version=self.genome_version,
                                     window_size=self.window_size)

    def output(self):
        super_output_path = super(Profile, self).output().path
        return GzipOutputFile(super_output_path)

    def requires(self):
        return [self._genome_windows_task, self._peaks_task]

    def run(self):
        logger = logging.getLogger('Profile')

        windows_task_output = self._genome_windows_task.output()
        peaks_task_output, __ = self._peaks_task.output()

        windows = pybedtools.BedTool(windows_task_output.path)
        peaks = pybedtools.BedTool(peaks_task_output.path)

        logger.debug('Sorting peaks')

        peaks = peaks.sort()

        logger.debug('Computing the intersection')
        intersection = windows.intersect(peaks, c=True, sorted=True)

        logger.debug('Outputting to {}'.format(self.output().path))
        with self.output().open('w') as output_file:
            _intersection_counts_to_wiggle(output_file,
                                           intersection,
                                           name=self.experiment_alias,
                                           description=os.path.basename(self.output().path),
                                           window_size=self.window_size,
                                           binarise=self.binary
                                           )

if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger('Profile').setLevel(logging.DEBUG)
    luigi.run()


