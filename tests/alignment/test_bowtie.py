import sh

from chipalign.alignment.implementations.bowtie import AlignedReadsBowtie, BowtieIndex
from chipalign.sequence import ShortReads
from tests.helpers.task_test import TaskTestCase
from chipalign.command_line_applications.samtools import samtools
import zipfile

class TestBowtieAlignment(TaskTestCase):
    _multiprocess_can_split_ = False

    # 00 makes sure index download test is run first (thus making 01 test below faster.)
    def test_00_index_download(self):

        # Use drosophila as it is smaller
        genome = 'dm6'

        task = BowtieIndex(genome_version=genome)
        self.build_task(task)

        expected_file_list = ['genome.1.bt2',
                              'genome.2.bt2',
                              'genome.3.bt2',
                              'genome.4.bt2',
                              'genome.rev.1.bt2',
                              'genome.rev.2.bt2']

        with zipfile.ZipFile(task.output().path, 'r') as zip_:
            self.assertListEqual(sorted(expected_file_list), sorted(zip_.namelist()))

    def test_01_bowtie_alignment_produces_readable_bam(self):


        srr_identifier = 'SRR3287554'  # Input_DMSO IMR90
        spot_limit = 10000

        # I know aligning to drosophila makes no sense but this will be so much faster..
        bowtie_task = AlignedReadsBowtie(genome_version='dm6',
                                         source='sra',
                                         accession=srr_identifier,
                                         limit=spot_limit,
                                         number_of_processes=1,
                                         seed=0)

        print('Building bowtie task')
        self.build_task(bowtie_task)

        bam_output_path = bowtie_task.output()[0].path

        try:
            samtools('quickcheck', bam_output_path)
        except sh.ErrorReturnCode:
            self.fail('Bowtie produced an invalid bam file')






