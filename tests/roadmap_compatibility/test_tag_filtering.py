from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import gzip
import os

from chipalign.alignment.filtering import FilteredReads
from chipalign.core.util import temporary_file, autocleaning_pybedtools
from chipalign.database.roadmap.downloaded_reads import RoadmapAlignedReads
from tests.helpers.external_resource import DownloadableExternalResource
from tests.helpers.task_test import TaskTestCase
from tests.roadmap_compatibility.roadmap_tag import roadmap_test


class FilteredReadsResource(DownloadableExternalResource):

    genome_version = None

    def __init__(self, associated_task_test_case, url, genome_version):
        super(FilteredReadsResource, self).__init__(associated_task_test_case, url)
        self.genome_version = genome_version

    def _obtain(self):
        with temporary_file(cleanup_on_exception=True) as gzip_tmp:
            self._fetch_resource(gzip_tmp)
            assert os.path.isfile(gzip_tmp)

            with temporary_file(cleanup_on_exception=True) as tmp_answer_file:
                with gzip.GzipFile(gzip_tmp, 'r') as _in:
                    with open(tmp_answer_file, 'w') as _out:
                        _out.writelines(_in)

                with autocleaning_pybedtools() as pybedtools:
                    with temporary_file(cleanup_on_exception=True) as answer_file:
                        answer_bedtool = pybedtools.BedTool(tmp_answer_file).slop(r=0, l=0,
                                                                                  genome=self.genome_version).sort()
                        answer_bedtool.saveas(answer_file)
                        self._relocate_to_output(answer_file)

class CachedRoadmapAlignedReads(RoadmapAlignedReads):
    def _output_directory(self):
        return TestTagFiltering.task_cache_directory()

@roadmap_test
class TestTagFiltering(TaskTestCase):

    _GENOME_VERSION = 'hg19'
    _TAG_LENGTH = 36

    def setUp(self):
        super(TestTagFiltering, self).setUp()

        filtered_reads_resource = FilteredReadsResource(self.__class__,
                                                        'http://egg2.wustl.edu/roadmap/data/byFileType/alignments/'
                                                        'unconsolidated/Class1Marks/'
                                                        'UCSD.H9.H3K56ac.YL238.filt.tagAlign.gz',
                                                        self._GENOME_VERSION)

        self.answer_file = filtered_reads_resource.get()

    def test_h3k56ac_can_be_reproduced(self):
        roadmap_aligned_reads = CachedRoadmapAlignedReads(url='http://genboree.org/EdaccData/Release-9/'
                                                              'experiment-sample/Histone_H3K56ac/H9_Cell_Line/'
                                                              'UCSD.H9.H3K56ac.YL238.bed.gz',
                                                          genome_version=self._GENOME_VERSION)

        filtered_reads_task = FilteredReads(genome_version=self._GENOME_VERSION,
                                            resized_length=self._TAG_LENGTH,
                                            alignment_task=roadmap_aligned_reads)

        self.build_task(filtered_reads_task)

        with autocleaning_pybedtools() as pybedtools:

            expected = pybedtools.BedTool(self.answer_file)
            actual = pybedtools.BedTool(filtered_reads_task.output().path)

            expected_head = '\n'.join(map(str, expected[:min(10, len(expected))]))
            actual_head = '\n'.join(map(str, actual[:min(10, len(actual))]))

            debug_msg = 'Expected:\n{}\nActual:\n{}\n'.format(expected_head, actual_head)
            self.assertEqual(len(expected), len(actual), 'Length of produced bed files differ: {} != {}.\n'
                                                         '{}'.format(len(expected), len(actual), debug_msg))

            key_func = lambda r: (r.chrom, r.start, r.end)

            prev_key = None
            expected_strand_counts = None
            actual_strand_counts = None

            for expected_row, actual_row in zip(expected, actual):
                # Get the key
                expected_key = key_func(expected_row)
                actual_key = key_func(actual_row)

                # And compare it
                self.assertTupleEqual(expected_key, actual_key)

                # Strand order in datasets is a bit undefined as .sort() by default does not care about them
                # therefore compare them in a sort-invariant way
                if expected_key != prev_key:
                    if prev_key is not None:
                        # Check the strand counts match
                        self.assertDictEqual(expected_strand_counts, actual_strand_counts,
                                             'Counts not equal for {!r}: {!r} != {!r}'.format(prev_key,
                                                                                              expected_strand_counts,
                                                                                              actual_strand_counts))
                    # Reset counts for new key
                    expected_strand_counts = {'+': 0, '-': 0}
                    actual_strand_counts = {'+': 0, '-': 0}
                    prev_key = expected_key

                # Add the counts
                expected_strand_counts[expected_row.strand] += 1
                actual_strand_counts[actual_row.strand] += 1

            # check the last one
            if prev_key is not None:
                self.assertDictEqual(expected_strand_counts, actual_strand_counts,
                                     'Counts not equal for {!r}: {!r} != {!r}'.format(prev_key,
                                                                                      expected_strand_counts,
                                                                                      actual_strand_counts))
