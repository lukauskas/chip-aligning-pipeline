from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import tarfile
import shutil
from chipalign.core.task import Task, luigi
from chipalign.quality_control.fragment_length import FragmentLength


class MACSResults(Task):

    input_task = luigi.Parameter()
    treatment_task = luigi.Parameter()

    fragment_length = luigi.Parameter(default='auto')

    OUTPUT_BASENAME = 'macs_out'

    @property
    def fragment_length_task(self):
        return FragmentLength(self.treatment_task)

    @property
    def _extension(self):
        return '7z'

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

    def _run(self):
        from chipalign.command_line_applications.macs import macs2
        from chipalign.command_line_applications.seven_z import seven_z

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

            logger.info('Compressing MACS output. This is expected to take some time.')

            archive_filename = 'archive.7z'

            files = ['{}_control_lambda.bdg'.format(output_basename),
                     '{}_treat_pileup.bdg'.format(output_basename),
                     '{}_peaks.narrowPeak'.format(output_basename),
                     '{}_summits.bed'.format(output_basename)]

            # Previously here we used uncompressed .tar files
            # while they were faster to compress, each file took 5GB
            # 7z will take longer should compress them to ~300GB (gzip does ~900GB)
            seven_z('a', archive_filename, *files)

            logger.info('Moving')
            shutil.move(archive_filename, output_abspath)
