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
from task import Task
import tempfile


class Peaks(Task):

    genome_version = BowtieAlignmentTask.genome_version
    experiment_accession = BowtieAlignmentTask.experiment_accession
    study_accession = BowtieAlignmentTask.study_accession
    experiment_alias = BowtieAlignmentTask.experiment_alias

    bowtie_seed = BowtieAlignmentTask.bowtie_seed

    pretrim_reads = BowtieAlignmentTask.pretrim_reads

    broad = luigi.BooleanParameter()

    @property
    def _bowtie_alignment_task(self):
        return BowtieAlignmentTask(genome_version=self.genome_version,
                                   experiment_accession=self.experiment_accession,
                                   study_accession=self.study_accession,
                                   experiment_alias=self.experiment_alias,
                                   bowtie_seed=self.bowtie_seed,
                                   pretrim_reads=self.pretrim_reads)

    def requires(self):
        return self._bowtie_alignment_task

    @property
    def parameters(self):
        alignment_params = self._bowtie_alignment_task.parameters
        alignment_params.append('broad' if self.broad else 'narrow')

        return alignment_params

    @property
    def _extension(self):
        return 'bed'

    def output(self):
        bed_output = super(Peaks, self).output()
        stderr_dump = luigi.File(bed_output.path + '.out')
        return bed_output, stderr_dump

    def run(self):

        from command_line_applications.macs import macs2
        logger = logging.getLogger('Peaks')

        temporary_directory = tempfile.mkdtemp(prefix='tmp-peaks')
        current_directory = os.getcwd()

        bam_input_file, __ = self.input()
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

        try:
            os.chdir(temporary_directory)
            logger.debug('Working in {}'.format(temporary_directory))
            logger.debug('Running macs')

            stderr_output = 'output.txt'

            if self.broad:
                broad_params = ['--broad']
            else:
                broad_params = []
            macs2_args = ['callpeak',
                          '-t', bam_input_abspath,
                          '-f', 'BAM',
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
        finally:
            os.chdir(current_directory)
            shutil.rmtree(temporary_directory)

if __name__ == '__main__':
    logging.getLogger('Peaks').setLevel(logging.DEBUG)
    logging.basicConfig()
    luigi.run()