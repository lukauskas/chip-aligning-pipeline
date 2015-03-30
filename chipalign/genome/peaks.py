from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import tarfile
import shutil
from chipalign.core.task import Task, luigi
from chipalign.genome.fragment_length import FragmentLength


class MACSResults(Task):

    input_task = luigi.Parameter()
    treatment_task = luigi.Parameter()

    fragment_length = luigi.Parameter(default='auto')

    OUTPUT_BASENAME = 'macs_out'


    def additional_parameters(self):
        additional_parameters = []

        if self.fragment_length_is_known():
            additional_parameters.append('f{}'.format(self.fragment_length_value()))

        return additional_parameters

    @property
    def parameters(self):
        additional_parameters = self.additional_parameters()
        return [self.input_task.task_class_friendly_name] + self.input_task.parameters \
               + [self.treatment_task.task_class_friendly_name] + self.treatment_task.parameters \
               + additional_parameters

    @property
    def fragment_length_task(self):
        return FragmentLength(self.treatment_task)

    @property
    def _extension(self):
        return 'tar.gz'

    def fragment_length_is_known(self):
        try:
            int(self.fragment_length)
        except ValueError:
            if self.fragment_length == 'auto':
                return False
            else:
                raise

        return True

    def fragment_length_value(self):
        if self.fragment_length == 'auto':
            return self.fragment_length_task.output().load()['fragment_lengths']['best']['length']
        else:
            return int(self.fragment_length)

    def requires(self):
        reqs = [self.input_task, self.treatment_task]

        if not self.fragment_length_is_known():
            reqs.append(self.fragment_length_task)

        return reqs

    def run(self):
        from chipalign.command_line_applications.macs import macs2

        logger = self.logger()

        fragment_length = self.fragment_length_value()

        logger.info('Using fragment length: {}'.format(fragment_length))

        treatment_abspath = os.path.abspath(self.treatment_task.output().path)
        input_abspath = os.path.abspath(self.input_task.output().path)

        output_abspath = os.path.abspath(self.output().path)
        self.ensure_output_directory_exists()

        with self.temporary_directory():

            output_basename = self.OUTPUT_BASENAME

            # ROADMAP code uses --shiftsize=fragment_length/2
            # Since then, the parameter has been deprecated, and replaced by --extsize
            # Extsize is double the shift size though, ergo equal to fragment length
            ext_size = fragment_length

            logger.info('Running MACS callpeak')
            macs2('callpeak',
                  '--nomodel',
                  '--extsize', ext_size,
                  '-B',
                  '--SPMR',
                  f='BED',
                  t=treatment_abspath,
                  c=input_abspath,
                  g='hs',
                  n=output_basename,
                  p=1e-2,
                  )

            logger.info('All done, creating tar archive')
            archive_filename = 'archive.tar.gz'
            with tarfile.open(archive_filename, 'w') as tf:
                tf.add('{}_control_lambda.bdg'.format(output_basename))
                tf.add('{}_treat_pileup.bdg'.format(output_basename))
                tf.add('{}_peaks.narrowPeak'.format(output_basename))
                tf.add('{}_summits.bed'.format(output_basename))

            logger.info('Moving')
            shutil.move(archive_filename, output_abspath)
