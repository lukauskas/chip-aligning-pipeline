from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from genome_alignment import BowtieAlignmentTask

class AlignedReadsMixin(object):

    genome_version = BowtieAlignmentTask.genome_version
    experiment_accession = BowtieAlignmentTask.experiment_accession
    study_accession = BowtieAlignmentTask.study_accession
    cell_type = BowtieAlignmentTask.cell_type
    data_track = BowtieAlignmentTask.data_track


    bowtie_seed = BowtieAlignmentTask.bowtie_seed
    pretrim_reads = BowtieAlignmentTask.pretrim_reads

    @property
    def friendly_name(self):
        return '{}-{}'.format(self.cell_type, self.data_track)

    @property
    def features_to_map_task(self):
        return BowtieAlignmentTask(genome_version=self.genome_version,
                                   experiment_accession=self.experiment_accession,
                                   study_accession=self.study_accession,
                                   cell_type=self.cell_type,
                                   data_track=self.data_track,
                                   bowtie_seed=self.bowtie_seed,
                                   pretrim_reads=self.pretrim_reads)
