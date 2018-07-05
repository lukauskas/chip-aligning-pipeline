from pandas.util.testing import assert_frame_equal

from chipalign.core.util import autocleaning_pybedtools
from chipalign.genome.mappability import GenomeMappabilityTrack, drop_unmappable, FullyMappableBins
from chipalign.genome.windows.genome_windows import NonOverlappingBins
from tests.helpers.task_test import TaskTestCase
from unittest import TestCase

class TestMappabilityTrackDownload(TaskTestCase):
    _multiprocess_can_split_ = False

    def test_mappability_is_downloaded_correctly(self):

        task = GenomeMappabilityTrack(
            genome_version='hg38',
            read_length=36
        )

        self.build_task(task)

        with autocleaning_pybedtools() as pybedtools:

            df = pybedtools.BedTool(task.output().path).to_dataframe()

        # Check that we have all the chromosomes
        self.assertEqual(df['chrom'].nunique(), 24)



class TestMappabilityFiltering(TestCase):

    def test_filtering_for_read_like_scenario(self):
        """
        Tests mappability filtering as it will be used in FilteredReads
        :return:
        """

        sample_reads = """
        chr1 1    36   partially_mappable  0 +
        chr1 2    37   fully_mappable_neg  0 -
        chr1 5    40   fully_mappable_pos 0 +
        chr2 5000 5035 not_mappable 0 +
        """

        expected = """
        chr1 2    37   fully_mappable_neg  0 -
        chr1 5    40   fully_mappable_pos 0 +
        """

        mappability = """
        chr1 2    40
        chr2 1000 3000
        """

        with autocleaning_pybedtools() as pybedtools:
            bd1 = pybedtools.BedTool(sample_reads, from_string=True)
            bd2 = pybedtools.BedTool(mappability, from_string=True)

            ans = drop_unmappable(bd1, bd2).to_dataframe()
            expected = pybedtools.BedTool(expected, from_string=True).to_dataframe()

        assert_frame_equal(ans, expected)

class TestFullyMappableBins(TaskTestCase):
    _multiprocess_can_split_ = False

    def test_bins_are_filtered_by_mappability(self):
        bins_task = NonOverlappingBins(
            genome_version='hg38',
            window_size=200
        )

        fm_bins_task = FullyMappableBins(
            bins_task=bins_task,
            read_length=36,
            max_ext_size=200
        )

        self.build_task(fm_bins_task)

        with autocleaning_pybedtools() as pybedtools:

            len_before = len(pybedtools.BedTool(bins_task.output().path))

            df = pybedtools.BedTool(fm_bins_task.output().path).to_dataframe()

        # Should be fewer bins
        self.assertLess(len(df), len_before)
        # Check that we have all the chromosomes
        self.assertEqual(df['chrom'].nunique(), 24)


