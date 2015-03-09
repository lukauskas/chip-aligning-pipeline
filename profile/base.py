from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import logging
import os
import luigi
import pybedtools
from genome_windows import NonOverlappingWindows
from peak_calling.macs import MacsPeaks
from profile.wigfile import WigFile
from task import Task

class ProfileBase(Task):
    """
    A base task that generates the profile of the peaks_task output over the
    range of non-overlapping genome windows of size window_size.

    If binary is set to False, the number of overlapping peaks_task outputs will be counted
    whereas if it is set to True, only a binary yes/no response will be returned.
    """

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
        return WigFile(genome_assembly=self.genome_version, window_size=self.window_size, path=super_output_path)

    def requires(self):
        return [self._genome_windows_task, self.peaks_task]

    def run(self):
        logger = logging.getLogger('Profile')

        windows_task_output = self._genome_windows_task.output()
        if isinstance(self.peaks_task.output(), list) and len(self.peaks_task.output()) == 2:
            peaks_task_output = self.peaks_task.output()[0]
        else:
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


def compute_profile(windows_task_output_abspath, peaks_task_output_abspath,
                    output, window_size, binarise, wigfile_name,  logger=None):

    def _debug(*args, **kwargs):
        if logger:
            logger.debug(*args, **kwargs)

    try:
        windows = pybedtools.BedTool(windows_task_output_abspath)
        peaks = pybedtools.BedTool(peaks_task_output_abspath)

        __, peaks_ext = os.path.splitext(peaks_task_output_abspath)
        if peaks_ext == '.bam':
            _debug('Peaks are in BAM format, converting to bed')
            # This is needed as peaks.sort() doesn't work for BAMs
            peaks = peaks.bam_to_bed()

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