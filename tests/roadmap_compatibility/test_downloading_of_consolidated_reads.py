from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import pybedtools

from chipalign.database.roadmap.downloaded_consolidated_reads import DownloadedConsolidatedReads
from tests.helpers.decorators import slow
from tests.helpers.task_test import TaskTestCase
from tests.roadmap_compatibility.roadmap_tag import roadmap_test


@roadmap_test
class TestDownloadedConsolidatedReadsAreReadable(TaskTestCase):

    @slow()
    def test_downloaded_E008_input_files_are_readable_by_pybedtools(self):

        task = DownloadedConsolidatedReads(cell_type='E008', track='Input', genome_version='hg19')
        self.build_task(task)

        output_bedtool = pybedtools.BedTool(task.output().path)
        # Should be more than zero reads in the bedtool
        self.assertGreater(output_bedtool.count(), 0)
