from chipalign.alignment.implementations.bowtie import AlignedReadsBowtie
from chipalign.sequence import ShortReads
from tests.helpers.task_test import TaskTestCase
from chipalign.command_line_applications import samtools

class TestBowtieAlignment(TaskTestCase):

    _GENOME = 'hg38'

    def test_bowtie_alignment_produces_readable_bam(self):


        srr_identifier = 'SRR3287554'  # Input_DMSO IMR90
        spot_limit = 10000

        fastq_sequence_task = ShortReads(source='sra', accession=srr_identifier,
                                         limit=spot_limit)

        bowtie_task = AlignedReadsBowtie(genome_version=self._GENOME,
                                         source='sra',
                                         accession=srr_identifier,
                                         limit=spot_limit,
                                         number_of_processes=1,
                                         seed=0)

        print('Building bowtie task')
        self.build_task(bowtie_task)

        bam_output_path = bowtie_task.output()[0].path

        quickcheck_output = samtools('quickcheck', bam_output_path)
        self.assertEqual(quickcheck_output, 0)




