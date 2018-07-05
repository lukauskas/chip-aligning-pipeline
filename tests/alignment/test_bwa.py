import sh

from chipalign.alignment.implementations.bwa import AlignedReadsBwa, BwaIndex
from tests.helpers.task_test import TaskTestCase
from chipalign.command_line_applications.samtools import samtools
import zipfile

class TestBwaAlignment(TaskTestCase):
    _multiprocess_can_split_ = False

    # 00 makes sure index download test is run first (thus making 01 test below faster.)
    def test_00_index_download(self):

        # Use drosophila as it is smaller
        genome = 'dm6'

        task = BwaIndex(genome_version=genome)
        self.build_task(task)

        expected_file_list = ['genome.fa.amb',
                              'genome.fa.ann',
                              'genome.fa.bwt',
                              'genome.fa.pac',
                              'genome.fa.sa']

        with zipfile.ZipFile(task.output().path, 'r') as zip_:
            self.assertListEqual(sorted(expected_file_list), sorted(zip_.namelist()))

    def test_01_bwa_alignment_produces_readable_bam(self):


        srr_identifier = 'SRR3287554'  # Input_DMSO IMR90
        spot_limit = 10000

        # I know aligning to drosophila makes no sense but this will be so much faster..
        bwa_task = AlignedReadsBwa(genome_version='dm6',
                                      source='sra',
                                      accession=srr_identifier,
                                      limit=spot_limit,
                                      number_of_processes=1)

        print('Building BWA task')
        self.build_task(bwa_task)

        bam_output_path = bwa_task.output()[0].path

        try:
            samtools('quickcheck', bam_output_path)
        except sh.ErrorReturnCode:
            self.fail('BWA produced an invalid BAM file')






