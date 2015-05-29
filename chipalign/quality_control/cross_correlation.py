from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import shutil
import luigi
from chipalign.core.task import Task


class CrossCorrelationPlot(Task):

    input_task = luigi.Parameter()

    def requires(self):
        return self.input_task

    @property
    def _extension(self):
        return 'pdf'

    @property
    def parameters(self):
        return self.input_task.parameters

    def run(self):
        from chipalign.command_line_applications.phantompeakqualtools import run_spp_nodups
        logger = self.logger()

        input_abspath = os.path.abspath(self.input().path)
        output_abspath = os.path.abspath(self.output().path)
        self.ensure_output_directory_exists()

        with self.temporary_directory():
            tmp_output = 'output.tmp'
            logger.debug('Running SPP')
            run_spp_nodups('-c={}'.format(input_abspath),
                           '-savp={}'.format(tmp_output),
                           '-odir=.')

            logger.debug('Relocating output')
            shutil.move(tmp_output, output_abspath)