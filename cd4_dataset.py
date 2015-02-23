from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import logging
import os

import luigi
import pysam
from genomic_profile import Profile
from itertools import imap, ifilterfalse
from collections import Counter
import pandas as pd

# SRA000206
METHYLATIONS = [
    #dict(experiment_accession='SRX000138', experiment_alias='CTCF', study_accession='SRP000201'),
    dict(experiment_accession='SRX000139', experiment_alias='H2A.Z', study_accession='SRP000201'),
    dict(experiment_accession='SRX000140', experiment_alias='H2BK5me1', study_accession='SRP000201'),
    dict(experiment_accession='SRX000141', experiment_alias='H3K27me1', study_accession='SRP000201'),
    dict(experiment_accession='SRX000142', experiment_alias='H3K27me2', study_accession='SRP000201'),
    dict(experiment_accession='SRX000143', experiment_alias='H3K27me3', study_accession='SRP000201'),
    dict(experiment_accession='SRX000144', experiment_alias='H3K36me1', study_accession='SRP000201'),
    dict(experiment_accession='SRX000145', experiment_alias='H3K36me3', study_accession='SRP000201'),
    dict(experiment_accession='SRX000146', experiment_alias='H3K4me1', study_accession='SRP000201'),
    dict(experiment_accession='SRX000147', experiment_alias='H3K4me2', study_accession='SRP000201'),
    dict(experiment_accession='SRX000148', experiment_alias='H3K4me3', study_accession='SRP000201'),
    dict(experiment_accession='SRX000149', experiment_alias='H3K79me1', study_accession='SRP000201'),
    dict(experiment_accession='SRX000150', experiment_alias='H3K79me2', study_accession='SRP000201'),
    dict(experiment_accession='SRX000151', experiment_alias='H3K79me3', study_accession='SRP000201'),
    dict(experiment_accession='SRX000152', experiment_alias='H3K9me1', study_accession='SRP000201'),
    dict(experiment_accession='SRX000153', experiment_alias='H3K9me2', study_accession='SRP000201'),
    dict(experiment_accession='SRX000154', experiment_alias='H3K9me3', study_accession='SRP000201'),
    dict(experiment_accession='SRX000155', experiment_alias='H3R2me1', study_accession='SRP000201'),
    dict(experiment_accession='SRX000156', experiment_alias='H3R2me2', study_accession='SRP000201'),
    dict(experiment_accession='SRX000157', experiment_alias='H4K20me1', study_accession='SRP000201'),
    dict(experiment_accession='SRX000158', experiment_alias='H4K20me3', study_accession='SRP000201'),
    dict(experiment_accession='SRX000159', experiment_alias='H4R3me2', study_accession='SRP000201'), # Also H2A
    #dict(experiment_accession='SRX000160', experiment_alias='Pol II', study_accession='SRP000201'),
]

# SRA000287
ACETYLATIONS = [
    dict(experiment_accession='SRX000354', experiment_alias='H2AK5ac', study_accession='SRP000200'), 
    dict(experiment_accession='SRX000355', experiment_alias='H2AK9ac', study_accession='SRP000200'),
    dict(experiment_accession='SRX000356', experiment_alias='H2BK120ac', study_accession='SRP000200'),
    dict(experiment_accession='SRX000357', experiment_alias='H2BK12ac', study_accession='SRP000200'),
    dict(experiment_accession='SRX000358', experiment_alias='H2BK20ac', study_accession='SRP000200'),
    dict(experiment_accession='SRX000359', experiment_alias='H2BK5ac', study_accession='SRP000200'),
    dict(experiment_accession='SRX000360', experiment_alias='H3K14ac', study_accession='SRP000200'),
    dict(experiment_accession='SRX000361', experiment_alias='H3K18ac', study_accession='SRP000200'),
    dict(experiment_accession='SRX000362', experiment_alias='H3K23ac', study_accession='SRP000200'),
    dict(experiment_accession='SRX000363', experiment_alias='H3K27ac', study_accession='SRP000200'),
    dict(experiment_accession='SRX000364', experiment_alias='H3K27me3', study_accession='SRP000200'),
    dict(experiment_accession='SRX000365', experiment_alias='H3K36ac', study_accession='SRP000200'),
    dict(experiment_accession='SRX000366', experiment_alias='H3K4ac', study_accession='SRP000200'),
    dict(experiment_accession='SRX000367', experiment_alias='H3K79me1', study_accession='SRP000200'),
    dict(experiment_accession='SRX000368', experiment_alias='H3K79me2', study_accession='SRP000200'),
    dict(experiment_accession='SRX000369', experiment_alias='H3K79me3', study_accession='SRP000200'),
    dict(experiment_accession='SRX000370', experiment_alias='H3K9ac', study_accession='SRP000200'),
    dict(experiment_accession='SRX000371', experiment_alias='H4K12ac', study_accession='SRP000200'),
    dict(experiment_accession='SRX000372', experiment_alias='H4K16ac', study_accession='SRP000200'),
    dict(experiment_accession='SRX000373', experiment_alias='H4K5ac', study_accession='SRP000200'),
    dict(experiment_accession='SRX000374', experiment_alias='H4K8ac', study_accession='SRP000200'),
    dict(experiment_accession='SRX000375', experiment_alias='H4K91ac', study_accession='SRP000200'),

]

# SRX103444
TRANSCRIPTION_FACTORS = [
    # BRD4
    dict(experiment_accession='SRX103444', experiment_alias='GSM823378_1',
         study_accession='PRJNA149083')
    ]

WINDOW_SIZE = 200

def tasks_for_genome(genome_version):
    for data_dict in METHYLATIONS + ACETYLATIONS:
        yield Profile(genome_version=genome_version,
                      pretrim_reads=True,
                      broad=True,
                      window_size=WINDOW_SIZE,
                      binary=True,
                      **data_dict)

    for data_dict in TRANSCRIPTION_FACTORS:
        yield Profile(genome_version=genome_version,
                      pretrim_reads=True,
                      broad=False,
                      window_size=WINDOW_SIZE,
                      binary=True,
                      **data_dict)


class CD4MasterTask(luigi.Task):

    genome_version = luigi.Parameter()

    def requires(self):
        return list(tasks_for_genome(self.genome_version))

    def complete(self):
        return all(map(lambda x: x.complete(), self.requires()))

class CD4AlignedReadLengthSummary(luigi.Task):

    genome_version = luigi.Parameter()

    def alignment_tasks(self):
        alignment_tasks = []
        profile_tasks = tasks_for_genome(self.genome_version)
        for profile_task in profile_tasks:
            alignment_task = profile_task.peaks_task.alignment_task
            alignment_tasks.append(alignment_task)
        return alignment_tasks

    def requires(self):
        return self.alignment_tasks()

    def output(self):
        return luigi.File('cd4_{}_query_length_counts.csv'.format(self.genome_version))

    def run(self):
        logger = logging.getLogger('CD4AlignedReadLengthSummary')
        df = []
        number_of_alignment_tasks = len(self.alignment_tasks())
        for i, alignment_task in enumerate(self.alignment_tasks(), start=1):
            bam_file = alignment_task.output()[0].path
            logger.debug('[{}/{}] Processing: {}'.format(i, number_of_alignment_tasks, bam_file))

            samfile_handle = pysam.Samfile(bam_file)

            mapped_reads = ifilterfalse(lambda x: x.is_unmapped, samfile_handle)
            query_lengths = imap(lambda x: x.query_length, mapped_reads)
            query_length_histogram = Counter(query_lengths)

            for key, value in query_length_histogram.iteritems():
                d = {'filename': os.path.basename(bam_file),
                     'query_length': key,
                     'count': value}
                df.append(d)

        df = pd.DataFrame(df)
        df = df.set_index('filename')

        with self.output().open('w') as f:
            df.to_csv(f)

if __name__ == '__main__':
    logging.getLogger('CD4AlignedReadLengthSummary').setLevel(logging.DEBUG)
    logging.basicConfig()
    luigi.run()