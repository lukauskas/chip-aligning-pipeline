from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from itertools import imap
import os
import luigi
import tempfile
import pybedtools
from chipalign.core.downloader import fetch
from chipalign.core.task import Task
from chipalign.core.util import clean_bedtool_history


class ChromatinStates(Task):

    genome_version = luigi.Parameter()
    cell_type = luigi.Parameter()
    number_of_states = luigi.IntParameter()

    regions_task = luigi.Parameter()

    @property
    def _extension(self):
        return 'bed.gz'

    @property
    def parameters(self):
        assert isinstance(self.regions_task, Task)
        return [self.cell_type, self.genome_version, self.number_of_states,
                self.regions_task.task_class_friendly_name] + self.regions_task.parameters

    def requires(self):
        return self.regions_task

    @property
    def _data_url(self):
        if self.genome_version == 'hg19':
            if self.number_of_states == 18:
                url_format = 'http://egg2.wustl.edu/roadmap/data/byFileType/chromhmmSegmentations/' \
                             'ChmmModels/core_K27ac/jointModel/final/{cell_type}_18_core_K27ac_mnemonics.bed.gz'
            elif self.number_of_states == 15:
                url_format = 'http://egg2.wustl.edu/roadmap/data/byFileType/chromhmmSegmentations/' \
                             'ChmmModels/coreMarks/jointModel/final/{cell_type}_15_coreMarks_mnemonics.bed.gz'
            else:
                raise ValueError('Unsupported number of states: {!r}'.format(self.number_of_states))
        else:
            raise ValueError('Unsupported genome version: {!r}'.format(self.genome_version))

        return url_format.format(cell_type=self.cell_type)

    def run(self):

        __, tmp_download_file = tempfile.mkstemp()
        input_file = self.input().path

        try:
            with open(tmp_download_file, 'w') as f:
                fetch(self._data_url, f)

            states = pybedtools.BedTool(tmp_download_file).sort()

            try:
                regions = pybedtools.BedTool(input_file)  # Assume sorted
                answer = regions.map(states, c=4, o='collapse')

                try:
                    with self.output().open('w') as output:
                        output.writelines(imap(str, answer))
                finally:
                    clean_bedtool_history(answer)
            finally:
                clean_bedtool_history(states)

        finally:
            try:
                os.unlink(tmp_download_file)
            except OSError:
                if os.path.isfile(tmp_download_file):
                    raise