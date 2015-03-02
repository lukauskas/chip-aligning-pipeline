from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import logging

import luigi
import pybedtools
from blacklist import BlacklistedRegions

from genome_windows import NonOverlappingWindows
from peak_calling.macs import MacsPeaks
from task import Task, GzipOutputFile
from peak_calling.rseg import RsegPeaks


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

def compute_profile(windows_task_output_abspath, peaks_task_output_abspath,
                    output, window_size, binarise, wigfile_name,  logger=None):

    def _debug(*args, **kwargs):
        if logger:
            logger.debug(*args, **kwargs)

    try:
        windows = pybedtools.BedTool(windows_task_output_abspath)
        peaks = pybedtools.BedTool(peaks_task_output_abspath)

        _debug('Sorting peaks')

        peaks = peaks.sort()

        _debug('Computing the intersection')
        intersection = windows.intersect(peaks, c=True, sorted=True)

        _debug('Outputting to {}'.format(output.path))
        with output.open('w') as output_file:
            _intersection_counts_to_wiggle(output_file,
                                           intersection,
                                           name=wigfile_name,
                                           description=os.path.basename(output.path),
                                           window_size=window_size,
                                           binarise=binarise
                                           )
    finally:
        pybedtools.cleanup()


class ProfileBase(Task):

    genome_version = MacsPeaks.genome_version

    window_size = NonOverlappingWindows.window_size
    binary = luigi.BooleanParameter()

    @property
    def peaks_task(self):
        raise NotImplementedError

    @property
    def friendly_name(self):
        raise NotImplementedError

    @property
    def parameters(self):
        parameters = self.peaks_task.parameters
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
        super_output_path = super(ProfileBase, self).output().path
        return GzipOutputFile(super_output_path)

    def requires(self):
        return [self._genome_windows_task, self.peaks_task]

    def run(self):
        logger = logging.getLogger('Profile')

        windows_task_output = self._genome_windows_task.output()
        peaks_task_output = self.peaks_task.output()
        if isinstance(peaks_task_output, list):
            assert len(peaks_task_output) == 2
            peaks_task_output = peaks_task_output[0]


        compute_profile(os.path.abspath(windows_task_output.path),
                        os.path.abspath(peaks_task_output.path),
                        self.output(),
                        self.window_size,
                        self.binary,
                        self.friendly_name,
                        logger=logger
                        )

class GenomicProfileBase(ProfileBase):
    experiment_accession = MacsPeaks.experiment_accession
    study_accession = MacsPeaks.study_accession
    experiment_alias = MacsPeaks.experiment_alias

    bowtie_seed = MacsPeaks.bowtie_seed
    pretrim_reads = MacsPeaks.pretrim_reads

    @property
    def friendly_name(self):
        return self.experiment_alias



class BlacklistProfile(ProfileBase):

    binary = True

    @property
    def peaks_task(self):
        return BlacklistedRegions(genome_version=self.genome_version)

    @property
    def friendly_name(self):
        return 'blacklist'

class MacsProfile(GenomicProfileBase):

    broad = MacsPeaks.broad

    @property
    def peaks_task(self):
        return MacsPeaks(genome_version=self.genome_version,
                     experiment_accession=self.experiment_accession,
                     study_accession=self.study_accession,
                     experiment_alias=self.experiment_alias,
                     bowtie_seed=self.bowtie_seed,
                     pretrim_reads=self.pretrim_reads,
                     broad=self.broad)

class RsegProfile(GenomicProfileBase):

    width_of_kmers = RsegPeaks.width_of_kmers
    prefix_length = RsegPeaks.prefix_length

    number_of_iterations = RsegPeaks.number_of_iterations


    @property
    def peaks_task(self):
        return RsegPeaks(genome_version=self.genome_version,
                         experiment_accession=self.experiment_accession,
                         study_accession=self.study_accession,
                         experiment_alias=self.experiment_alias,
                         bowtie_seed=self.bowtie_seed,
                         pretrim_reads=self.pretrim_reads,
                         width_of_kmers=self.width_of_kmers,
                         prefix_length=self.prefix_length,
                         number_of_iterations=self.number_of_iterations)


if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger('Profile').setLevel(logging.DEBUG)
    luigi.run()


