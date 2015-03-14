from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from genome_alignment import BowtieAlignmentTask
from task import Task
import luigi

class PeaksBase(Task):

    genome_version = BowtieAlignmentTask.genome_version
    experiment_accession = BowtieAlignmentTask.experiment_accession
    study_accession = BowtieAlignmentTask.study_accession
    cell_type = BowtieAlignmentTask.cell_type
    data_track = BowtieAlignmentTask.data_track

    bowtie_seed = BowtieAlignmentTask.bowtie_seed

    pretrim_reads = BowtieAlignmentTask.pretrim_reads


    @property
    def alignment_task(self):
        return BowtieAlignmentTask(genome_version=self.genome_version,
                                   experiment_accession=self.experiment_accession,
                                   study_accession=self.study_accession,
                                   cell_type=self.cell_type,
                                   data_track=self.data_track,
                                   bowtie_seed=self.bowtie_seed,
                                   pretrim_reads=self.pretrim_reads)

    def requires(self):
        return [self.alignment_task]

    @property
    def parameters(self):
        return self.alignment_task.parameters

    @property
    def _extension(self):
        return 'bed'

    def output(self):
        bed_output = super(PeaksBase, self).output()
        stderr_dump = luigi.File(bed_output.path + '.out')
        return bed_output, stderr_dump

    def run(self):
        raise NotImplementedError
