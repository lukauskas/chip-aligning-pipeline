from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import logging
import multiprocessing
import os
import re
import gzip
import numpy as np

import luigi
import pysam
import time
from profile.blacklist import BlacklistProfile
from profile.raw import RawProfile
from profile.peak_caller import MacsProfile, RsegProfile
from itertools import imap, ifilterfalse
from collections import Counter
import pandas as pd
import itertools

# SRA000206
from tss import TssProfile

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
WIDTH_OF_KMERS=20
NUMBER_OF_RSEG_ITERATIONS=20

VALID_CHROMOSOMES = {'chr{}'.format(x) for x in [range(1, 23) + 'X' + 'Y']}

def tasks_for_genome(genome_version, binarisation_method):
    MARKS_MACS_FAILS_FOR ={'H4R3me2', 'H2AK5ac',
                            'H2BK12ac', 'H3K14ac', 'H3K23ac', 'H3K36ac',
                            'H3K9ac', 'H4K5ac', 'H4K8ac'}

    for data_dict in METHYLATIONS + ACETYLATIONS:

        if binarisation_method == 'macs':
            if data_dict['experiment_alias'] not in MARKS_MACS_FAILS_FOR:
                yield MacsProfile(genome_version=genome_version,
                              pretrim_reads=True,
                              broad=True,
                              window_size=WINDOW_SIZE,
                              binary=True,
                              **data_dict)
        elif binarisation_method == 'rseg':
            yield RsegProfile(genome_version=genome_version,
                              pretrim_reads=True,
                              window_size=WINDOW_SIZE,
                              binary=True,
                              width_of_kmers=WIDTH_OF_KMERS,
                              number_of_iterations=NUMBER_OF_RSEG_ITERATIONS,
                              **data_dict
                             )
        elif binarisation_method == 'raw':
            yield RawProfile(genome_version=genome_version,
                              pretrim_reads=True,
                              window_size=WINDOW_SIZE,
                              binary=False,
                              **data_dict
                             )
        else:
            raise NotImplementedError('Binarisation method {!r} not implemented'.format(binarisation_method))

    for data_dict in TRANSCRIPTION_FACTORS:
        yield MacsProfile(genome_version=genome_version,
                      pretrim_reads=True,
                      broad=False,
                      window_size=WINDOW_SIZE,
                      binary=True,
                      **data_dict)

    yield BlacklistProfile(genome_version=genome_version,
                           window_size=WINDOW_SIZE)

    yield TssProfile(genome_version=genome_version,
                     extend_5_to_3=4000,
                     extend_3_to_5=2000,
                     window_size=WINDOW_SIZE)

class CD4MasterTask(luigi.Task):

    genome_version = luigi.Parameter()
    binarisation_method = luigi.Parameter()

    def requires(self):
        return list(tasks_for_genome(self.genome_version, binarisation_method=self.binarisation_method))

    def output(self):
        return luigi.File('cd4_profile.{}.{}.csv.gz'.format(self.binarisation_method, self.genome_version))

    def run(self):
        logger = logging.getLogger('CD4MasterTask')

        df = pd.DataFrame()

        index = None
        logger.debug('Compiling to dataframe')

        for task in self.requires():
            output = task.output()
            alias = task.friendly_name
            logger.debug('Start {}'.format(alias))
            series = output.to_pandas_series(VALID_CHROMOSOMES)
            ix = series.index
            logger.debug('Finished parsing series')
            if index is None:
                index = ix
            else:
                if not index.equals(ix):
                    logger.error('Index for {} does not match previous indexes'.format(alias))
                    set_index_chromosomes = set(index.get_level_values('chromosome'))
                    set_ix_chromosomes = set(ix.get_level_values('chromosome'))
                    logger.debug('Chromosome ixs equal? {}'.format(set_index_chromosomes == set_ix_chromosomes))
                    logger.debug('Index: {}'.format(sorted(set_index_chromosomes)))
                    logger.debug('Ix: {}'.format(sorted(set_ix_chromosomes)))

                    raise AssertionError('indices don\'t match')

            start_time = time.time()
            df[alias] = series.values
            end_time = time.time()
            logger.debug('End {}, took: {}s'.format(alias, end_time - start_time))

        df = df.set_index(index)

        logger.debug('Dumping to CSV')
        with gzip.GzipFile(self.output().path, 'w') as f:
            df.to_csv(f)



if __name__ == '__main__':
    logging.getLogger('CD4MasterTask').setLevel(logging.DEBUG)
    logging.basicConfig()
    luigi.run()