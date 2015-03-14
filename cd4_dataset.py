from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import logging
import gzip

import luigi
import time
from profile.raw import RawProfile
from profile.peak_caller import MacsProfile, RsegProfile
import pandas as pd
from profile.tss import ReadsPerTss
from task import Task

# SRA000206
from tss import TssGenomeWideProfile
def methylations():
    return [
        #dict(experiment_accession='SRX000138', data_track='CTCF', study_accession='SRP000201'),
        dict(experiment_accession='SRX000139', data_track='H2A.Z', study_accession='SRP000201'),
        dict(experiment_accession='SRX000140', data_track='H2BK5me1', study_accession='SRP000201'),
        dict(experiment_accession='SRX000141', data_track='H3K27me1', study_accession='SRP000201'),
        dict(experiment_accession='SRX000142', data_track='H3K27me2', study_accession='SRP000201'),
        # dict(experiment_accession='SRX000143', data_track='H3K27me3', study_accession='SRP000201'),
        dict(experiment_accession='SRX000144', data_track='H3K36me1', study_accession='SRP000201'),
        dict(experiment_accession='SRX000145', data_track='H3K36me3', study_accession='SRP000201'),
        dict(experiment_accession='SRX000146', data_track='H3K4me1', study_accession='SRP000201'),
        dict(experiment_accession='SRX000147', data_track='H3K4me2', study_accession='SRP000201'),
        dict(experiment_accession='SRX000148', data_track='H3K4me3', study_accession='SRP000201'),
        # dict(experiment_accession='SRX000149', data_track='H3K79me1', study_accession='SRP000201'),
        # dict(experiment_accession='SRX000150', data_track='H3K79me2', study_accession='SRP000201'),
        # dict(experiment_accession='SRX000151', data_track='H3K79me3', study_accession='SRP000201'),
        dict(experiment_accession='SRX000152', data_track='H3K9me1', study_accession='SRP000201'),
        dict(experiment_accession='SRX000153', data_track='H3K9me2', study_accession='SRP000201'),
        dict(experiment_accession='SRX000154', data_track='H3K9me3', study_accession='SRP000201'),
        dict(experiment_accession='SRX000155', data_track='H3R2me1', study_accession='SRP000201'),
        dict(experiment_accession='SRX000156', data_track='H3R2me2', study_accession='SRP000201'),
        dict(experiment_accession='SRX000157', data_track='H4K20me1', study_accession='SRP000201'),
        dict(experiment_accession='SRX000158', data_track='H4K20me3', study_accession='SRP000201'),
        dict(experiment_accession='SRX000159', data_track='H4R3me2', study_accession='SRP000201'), # Also H2A
        # These last four come from the acetylations study (SRA000287)
        dict(experiment_accession='SRX000367', data_track='H3K79me1', study_accession='SRP000200'),
        dict(experiment_accession='SRX000368', data_track='H3K79me2', study_accession='SRP000200'),
        dict(experiment_accession='SRX000369', data_track='H3K79me3', study_accession='SRP000200'),
        dict(experiment_accession='SRX000364', data_track='H3K27me3', study_accession='SRP000200'),

        #dict(experiment_accession='SRX000160', data_track='Pol II', study_accession='SRP000201'),
    ]

# SRA000287
def acetylations():
    return [
        dict(experiment_accession='SRX000354', data_track='H2AK5ac', study_accession='SRP000200'),
        dict(experiment_accession='SRX000355', data_track='H2AK9ac', study_accession='SRP000200'),
        dict(experiment_accession='SRX000356', data_track='H2BK120ac', study_accession='SRP000200'),
        dict(experiment_accession='SRX000357', data_track='H2BK12ac', study_accession='SRP000200'),
        dict(experiment_accession='SRX000358', data_track='H2BK20ac', study_accession='SRP000200'),
        dict(experiment_accession='SRX000359', data_track='H2BK5ac', study_accession='SRP000200'),
        dict(experiment_accession='SRX000360', data_track='H3K14ac', study_accession='SRP000200'),
        dict(experiment_accession='SRX000361', data_track='H3K18ac', study_accession='SRP000200'),
        dict(experiment_accession='SRX000362', data_track='H3K23ac', study_accession='SRP000200'),
        dict(experiment_accession='SRX000363', data_track='H3K27ac', study_accession='SRP000200'),
        dict(experiment_accession='SRX000365', data_track='H3K36ac', study_accession='SRP000200'),
        dict(experiment_accession='SRX000366', data_track='H3K4ac', study_accession='SRP000200'),
        dict(experiment_accession='SRX000370', data_track='H3K9ac', study_accession='SRP000200'),
        dict(experiment_accession='SRX000371', data_track='H4K12ac', study_accession='SRP000200'),
        dict(experiment_accession='SRX000372', data_track='H4K16ac', study_accession='SRP000200'),
        dict(experiment_accession='SRX000373', data_track='H4K5ac', study_accession='SRP000200'),
        dict(experiment_accession='SRX000374', data_track='H4K8ac', study_accession='SRP000200'),
        dict(experiment_accession='SRX000375', data_track='H4K91ac', study_accession='SRP000200'),

    ]

# SRX103444
def transcription_factors():
    return [
        # BRD4
        dict(experiment_accession='SRX103444', data_track='BRD4',
             study_accession='PRJNA149083')
        ]

def open_chromatin():
    return [
        # DNase-seq
        dict(experiment_accession='SRX100962', data_track='DNase', study_accession='PRJNA34535')
    ]

WINDOW_SIZE = 200
WIDTH_OF_KMERS=20
NUMBER_OF_RSEG_ITERATIONS=20

VALID_CHROMOSOMES = {'chr{}'.format(x) for x in range(1, 23) + ['X', 'Y']}

def _tasks_for_genome(genome_version, binarisation_method):
    MARKS_MACS_FAILS_FOR ={'H4R3me2', 'H2AK5ac',
                            'H2BK12ac', 'H3K14ac', 'H3K23ac', 'H3K36ac',
                            'H3K9ac', 'H4K5ac', 'H4K8ac'}

    for data_dict in methylations() + acetylations():

        if binarisation_method == 'macs':
            if data_dict['experiment_alias'] not in MARKS_MACS_FAILS_FOR:
                yield MacsProfile(genome_version=genome_version,
                              pretrim_reads=True,
                              broad=True,
                              window_size=WINDOW_SIZE,
                              binary=True,
                              cell_type='CD4+',
                              **data_dict)
        elif binarisation_method == 'rseg':
            yield RsegProfile(genome_version=genome_version,
                              pretrim_reads=True,
                              window_size=WINDOW_SIZE,
                              binary=True,
                              width_of_kmers=WIDTH_OF_KMERS,
                              number_of_iterations=NUMBER_OF_RSEG_ITERATIONS,
                              cell_type='CD4+',
                              **data_dict
                             )
        elif binarisation_method == 'raw':
            yield RawProfile(genome_version=genome_version,
                              pretrim_reads=True,
                              window_size=WINDOW_SIZE,
                              binary=False,
                              extend_to_length=150,
                              cell_type='CD4+',
                              **data_dict
                             )
        else:
            raise NotImplementedError('Binarisation method {!r} not implemented'.format(binarisation_method))

class CD4HistoneModifications(Task):

    genome_version = luigi.Parameter()
    binarisation_method = luigi.Parameter()

    def requires(self):
        return list(_tasks_for_genome(self.genome_version, binarisation_method=self.binarisation_method))

    @property
    def extension(self):
        return 'csv.gz'

    @property
    def parameters(self):
        return [self.binarisation_method, self.genome_version]

    def run(self):
        logger = self.logger()

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

class CD4NormalisedHistoneModifications(Task):

    genome_version = luigi.Parameter()

    rolling_window_size = luigi.IntParameter(default=50)
    normalisation_method = luigi.Parameter(default='median')
    additive_smoothing_constant = luigi.Parameter(default=0.1)

    def requires(self):
        return CD4HistoneModifications(genome_version=self.genome_version, binarisation_method='raw')

    @property
    def _extension(self):
        return 'csv.gz'

    @property
    def parameters(self):
        return [self.genome_version, self.normalisation_method, 'w{}'.format(self.rolling_window_size), 's{}'.format(self.additive_smoothing_constant)]


    def run(self):
        logger = self.logger()

        logger.debug('Reading the CSV from input')
        histone_modification_data = pd.read_csv(self.input().path, compression='gzip').set_index(['chromosome', 'window_id'])

        logger.debug('Adding pseudocount')
        histone_modification_data += self.additive_smoothing_constant

        logger.debug('Rolling means over data')
        histone_modification_data = histone_modification_data.groupby(level='chromosome').apply(lambda x: pd.rolling_mean(x, self.rolling_window_size, center=True))
        histone_modification_data = histone_modification_data.dropna()

        logger.debug('Normalising data using {}'.format(self.normalisation_method))
        if self.normalisation_method == 'median':
            histone_modification_data /= histone_modification_data.median()
        elif self.normalisation_method == 'mean':
            histone_modification_data /= histone_modification_data.mean()
        elif self.normalisation_method == 'sum':
            histone_modification_data /= histone_modification_data.sum()
        elif self.normalisation_method == 'none' or self.normalisation_method is None \
                or self.normalisation_method == 'None':
            pass
        else:
            raise Exception("Unsupported normalisation method {}".format(self.normalisation_method))

        logger.debug('Dumping to CSV')
        with self.output().open('w') as output:
            histone_modification_data.to_csv(output, index=True, header=True)

        logger.debug('Done')

class CD4InputEstimate(Task):

    genome_version = CD4NormalisedHistoneModifications.genome_version
    rolling_window_size = CD4NormalisedHistoneModifications.rolling_window_size
    normalisation_method = CD4NormalisedHistoneModifications.normalisation_method
    additive_smoothing_constant = CD4NormalisedHistoneModifications.additive_smoothing_constant

    def _normalised_histone_mods_task(self):
        return CD4NormalisedHistoneModifications(genome_version=self.genome_version,
                                                 rolling_window_size=self.rolling_window_size,
                                                 normalisation_method=self.normalisation_method,
                                                 additive_smoothing_constant=self.additive_smoothing_constant)
    def requires(self):
        return self._normalised_histone_mods_task()

    @property
    def parameters(self):
        return self._normalised_histone_mods_task().parameters

    @property
    def _extension(self):
        return 'csv.gz'

    def run(self):
        normalised_histone_modification_data = pd.read_csv(self.input().path, compression='gzip').set_index(['chromosome', 'window_id'])

        logger = self.logger()

        logger.debug('Taking the median of means')
        median_of_rolling_means = normalised_histone_modification_data.median(axis=1)

        logger.debug('Dumping to CSV')
        with self.output().open('w') as output:
            median_of_rolling_means.to_csv(output, index=True, header=True)

        logger.debug('Done')

class CD4TssCountsDataFrame(Task):

    genome_version = luigi.Parameter()

    extend_5_to_3 = luigi.IntParameter(default=2000)
    extend_3_to_5 = luigi.IntParameter(default=2000)
    merge = luigi.BooleanParameter(default=0)

    pretrim_reads = luigi.BooleanParameter(default=True)
    extend_to_length = luigi.IntParameter(150)

    @property
    def _extension(self):
        return 'csv.gz'

    def requires(self):
        reqs = []

        for d in acetylations() + methylations() + open_chromatin() + transcription_factors():
            task = ReadsPerTss(genome_version=self.genome_version,
                               extend_5_to_3=self.extend_5_to_3,
                               extend_3_to_5=self.extend_3_to_5,
                               merge=self.merge,
                               binary=False,
                               pretrim_reads=self.pretrim_reads,
                               extend_to_length=self.extend_to_length,
                               cell_type='CD4+',
                               **d)
            reqs.append(task)

        return reqs

    @property
    def parameters(self):
        return [self.genome_version, self.extend_5_to_3, self.extend_3_to_5, self.merge,
                self.pretrim_reads, self.extend_to_length]

    def run(self):
        series_list = []

        for task in self.requires():
            d = pd.read_csv(task.output().path, compression='gzip')
            series = d.set_index(['chromosome', 'start', 'end', 'name', 'score'])['value']
            series.name = task.experiment_alias

            series_list.append(series)

        df = pd.concat(series_list, axis=1).reset_index()

        with self.output().open('w') as f:
            df.to_csv(f, index=False)

if __name__ == '__main__':
    for class_ in [CD4NormalisedHistoneModifications, CD4HistoneModifications, CD4InputEstimate]:
        class_.logger().setLevel(logging.DEBUG)

    logging.basicConfig()
    luigi.run()