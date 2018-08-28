import gzip
import shutil

import luigi
import os

from chipalign.core.task import Task
from chipalign.core.util import timed_segment, autocleaning_pybedtools, temporary_file
from chipalign.signal.peaks import MACSResults


class WindowsAroundSummits(Task):

    genome_version = luigi.Parameter()
    window_size = luigi.IntParameter()
    slop = luigi.IntParameter()

    macs_task = luigi.Parameter()

    @property
    def task_class_friendly_name(self):
        return 'WAS'

    @property
    def _extension(self):
        return 'bed.gz'

    def requires(self):
        return self.macs_task

    def _run(self):
        from chipalign.command_line_applications.archiving import seven_z
        macs_callpeaks_files_abspath = os.path.abspath(self.macs_task.output().path)
        self.ensure_output_directory_exists()
        output_abspath = os.path.abspath(self.output().path)

        with self.temporary_directory():
            macs_basename = MACSResults.OUTPUT_BASENAME
            summits_file = '{}_summits.bed'.format(macs_basename)

            with timed_segment('Extracting MACS2 result'):
                seven_z('x', macs_callpeaks_files_abspath,
                        summits_file)

            output_file = 'output.bed'

            with timed_segment('Making summit windows'):
                with autocleaning_pybedtools() as pybedtools:
                    bdt = pybedtools.BedTool(summits_file)
                    bdt_slop = bdt.slop(b=self.slop, genome=self.genome_version)

                    windows = bdt_slop.window_maker(genome=self.genome_version,
                                                    w=self.window_size,
                                                    i='srcwinnum')
                    windows = windows.sort()
                    windows.saveas(output_file)

            with temporary_file() as gzip_tmp:
                with open(output_file, 'rb') as input_file:
                    with gzip.GzipFile(gzip_tmp, 'w') as gzipped_file:
                        gzipped_file.writelines(input_file)

                shutil.move(gzip_tmp, output_abspath)