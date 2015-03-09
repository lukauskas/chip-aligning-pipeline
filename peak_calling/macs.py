from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import logging
import os
import luigi
import shutil
import sh
from genome_alignment import BowtieAlignmentTask
from peak_calling.base import PeaksBase
import tempfile
from util import temporary_directory


class MacsPeaks(PeaksBase):

    broad = luigi.BooleanParameter()
    macs_q_value_threshold = luigi.FloatParameter(default=0.01)

    @property
    def parameters(self):
        parameters = super(MacsPeaks, self).parameters
        parameters.append('broad' if self.broad else 'narrow')
        if self.macs_q_value_threshold != 0.01:
            parameters.append('q{}'.format(self.macs_q_value_threshold))

        return parameters

    def run(self):

        from command_line_applications.macs import macs2
        logger = logging.getLogger('Peaks')


        bam_input_file, __ = self.input()[0]
        bam_input_abspath = os.path.abspath(bam_input_file.path)

        bed_output, stdout_output = self.output()
        stdout_output_abspath = os.path.abspath(stdout_output.path)
        bed_output_abspath = os.path.abspath(bed_output.path)
        logger.debug('Ensuring output directory exists')
        # Make sure to create directories
        try:
            os.makedirs(os.path.dirname(stdout_output_abspath))
        except OSError:
            if not os.path.isdir(os.path.dirname(stdout_output_abspath)):
                raise

        try:
            os.makedirs(os.path.dirname(bed_output_abspath))
        except OSError:
            if not os.path.isdir(os.path.dirname(bed_output_abspath)):
                raise

        logger.debug('Calling peaks for {}'.format(bam_input_abspath))

        with temporary_directory(prefix='tmp-macs-', logger=logger):
            logger.debug('Running macs')

            stderr_output = 'output.txt'

            if self.broad:
                broad_params = ['--broad']
            else:
                broad_params = []
            macs2_args = ['callpeak',
                          '-t', bam_input_abspath,
                          '-f', 'BAM',
                          '-q', self.macs_q_value_threshold,
                          '-g', 'hs'] + broad_params
            try:
                macs2(*macs2_args,
                      _err=stderr_output)
            except sh.ErrorReturnCode as e:
                # Rerun command without output redirection, so we can capture it in the exception
                macs2(*macs2_args)

            logger.debug('Moving files')
            shutil.move(stderr_output, stdout_output_abspath)
            if self.broad:
                shutil.move('NA_peaks.broadPeak', bed_output_abspath)
            else:
                shutil.move('NA_peaks.narrowPeak', bed_output_abspath)

if __name__ == '__main__':
    logging.getLogger('Peaks').setLevel(logging.DEBUG)
    logging.basicConfig()
    luigi.run()