from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import unittest
import tempfile
import luigi
import shutil
from chipalign.core.downloader import fetch
from chipalign.genome.genome_alignment import DownloadedConsolidatedReads
from chipalign.genome.genome_signal import Signal
from chipalign.roadmap_data.downloaded_signal import DownloadedSignal
from tests.roadmap_compatibility.roadmap_tag import roadmap_test
from chipalign.core.util import _CHIPALIGN_OUTPUT_DIRECTORY_ENV_VAR, temporary_file
from itertools import izip

@roadmap_test
class TestMacsPileup(unittest.TestCase):

    _CELL_TYPE = 'E008'
    _TRACK = 'H3K56ac'
    _GENOME_VERSION = 'hg19'
    _CHROMOSOMES = 'male'

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix='tests-temp')
        os.environ[_CHIPALIGN_OUTPUT_DIRECTORY_ENV_VAR] = self.temp_dir

    def tearDown(self):
        try:
            shutil.rmtree(self.temp_dir)
        except OSError:
            if os.path.isdir(self.temp_dir):
                raise

    @classmethod
    def setUpClass(cls):
        from chipalign.command_line_applications.ucsc_suite import bigWigToBedGraph
        from chipalign.command_line_applications.common import sort

        if cls._GENOME_VERSION != 'hg19':
            raise NotImplementedError('Cannot do this for non hg19')

        answer_url = 'http://egg2.wustl.edu/roadmap/data/byFileType/signal/consolidated/macs2signal/pval/' \
                     '{}-{}.pval.signal.bigwig'.format(cls._CELL_TYPE, cls._TRACK)

        with temporary_file(prefix='tmp-signal', suffix='.bigWig') as tmp_bigwig_file:

            with open(tmp_bigwig_file, 'w') as f:
                fetch(answer_url, f)

            with temporary_file(prefix='tmp-signal', suffix='.bdg') as tmp_bdg_file:
                bigWigToBedGraph(tmp_bigwig_file, tmp_bdg_file)

                __, cls.answer_file = tempfile.mkstemp(prefix='tmp-TestMacsPileup-answer')
                sort(tmp_bdg_file, '-k1,1', '-k2,2n', '-k3,3n', '-k5,5n',
                     '-o', cls.answer_file)

    @classmethod
    def tearDownClass(cls):
        try:
            os.unlink(cls.answer_file)
        except OSError:
            if os.path.isfile(cls.answer_file):
                raise

    def test_downloaded_signal_task_downloads_correctly(self):

        ds = DownloadedSignal(cell_type=self._CELL_TYPE,
                              track=self._TRACK,
                              genome_version=self._GENOME_VERSION,
                              chromosomes=self._CHROMOSOMES)

        luigi.build([ds], local_scheduler=True)

        self.assertTrue(ds.complete())


        with ds.output().open('r') as actual:
            with open(self.answer_file) as expected:

                for expected_row, actual_row in izip(expected, actual):
                    self.assertEquals(expected_row, actual_row)

                # Check that files were read completely (`izip` stops when one of them stops)
                self.assertRaises(StopIteration, actual.next)
                self.assertRaises(StopIteration, expected.next)

    def test_can_reproduce_signal_track(self):


        input_reads = DownloadedConsolidatedReads(genome_version=self._GENOME_VERSION,
                                                  cell_type=self._CELL_TYPE,
                                                  track='Input',
                                                  chromosomes=self._CHROMOSOMES
                                                  )

        track_reads = DownloadedConsolidatedReads(genome_version=self._GENOME_VERSION,
                                                  cell_type=self._CELL_TYPE,
                                                  track=self._TRACK,
                                                  chromosomes=self._CHROMOSOMES
                                                  )

        st = Signal(input_task=input_reads, treatment_task=track_reads)

        luigi.build([st], local_scheduler=True)

        self.assertTrue(st.complete())

        with st.output().oopen('r') as actual:
            with open(self.answer_file) as expected:
                for expected_row, actual_row in izip(expected, actual):
                    self.assertEquals(expected_row, actual_row)

                # Check that files were read completely (`izip` stops when one of them stops)
                self.assertRaises(StopIteration, actual.next)
                self.assertRaises(StopIteration, expected.next)

