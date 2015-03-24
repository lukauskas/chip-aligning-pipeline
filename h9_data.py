from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import logging
import luigi
from downloaded_signal import DownloadableSignalTracks, DownloadedSignal
from genome_alignment import ConsolidatedReads

data = [
    {'cell_type': u'H9', 'experiment_accession': 'SRX670813', 'study_accession': 'PRJNA257661', 'data_track': 'BRD4'},
    {'cell_type': u'H9', 'experiment_accession': 'SRX670811', 'study_accession': 'PRJNA257661', 'data_track': 'IgG'},
]

class BRD4Alignments(luigi.Task):
    genome_version = luigi.Parameter()
    aligner = luigi.Parameter(default='pash')
    chromosomes = luigi.Parameter(default='female')

    max_sequencing_depth = ConsolidatedReads.max_sequencing_depth


    def requires(self):
        brd4_signal = ['SRR1537736', 'SRR1537737'] # GSM1466835 BRD4 Vehicle- treated 6h
        brd4_control = ['SRR1537734']  # IgG GSM1466833

        return [ConsolidatedReads(genome_version=self.genome_version,
                                  aligner=self.aligner,
                                  srr_identifiers=brd4_signal,
                                  chromosomes=self.chromosomes,
                                  max_sequencing_depth=self.max_sequencing_depth),
                ConsolidatedReads(genome_version=self.genome_version,
                                  aligner=self.aligner,
                                  srr_identifiers=brd4_control,
                                  chromosomes=self.chromosomes,
                                  max_sequencing_depth=self.max_sequencing_depth)
                ]

    def complete(self):
        return all(map(lambda x: x.complete(), self.requires()))

class H3K56acRefData(luigi.Task):

    genome_version = luigi.Parameter()
    aligner = luigi.Parameter(default='pash')

    chromosomes = luigi.Parameter(default='female')

    max_sequencing_depth = ConsolidatedReads.max_sequencing_depth

    def requires(self):
        srrs = ['SRR179694', 'SRR097968']

        return [ConsolidatedReads(genome_version=self.genome_version,
                                  aligner=self.aligner,
                                  srr_identifiers=srrs,
                                  chromosomes=self.chromosomes,
                                  max_sequencing_depth=self.max_sequencing_depth)]

    def complete(self):
        return all(map(lambda x: x.complete(), self.requires()))

class AllSignals(luigi.Task):

    genome_version = luigi.Parameter(default='hg19')
    chromosomes = luigi.Parameter(default='female')
    cell_type = luigi.Parameter(default='E008')

    def requires(self):
        try:
            return self.__requires_cache
        except AttributeError:
            downloadable_signals = DownloadableSignalTracks(genome_version=self.genome_version, cell_type=self.cell_type)
            luigi.build([downloadable_signals])

            tracks = downloadable_signals.output().load()
            requires = [DownloadedSignal(genome_version=self.genome_version, cell_type=self.cell_type,
                                         chromosomes=self.chromosomes, track=track) for track in tracks]
            self.__requires_cache = requires

            return self.__requires_cache

    def complete(self):
        return all([r.complete() for r in self.requires()])


if __name__ == '__main__':
    logging.basicConfig()
    luigi.run()
