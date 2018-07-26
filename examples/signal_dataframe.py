"""
Example task on how to obtain data for a set of TFs from ENCODE and
associated histone modifications from ROADMAP

Prior to running this example, just like all examples ensure that luigi scheduler is running,
by typing:

    luigid

You can then run this by

python signal_dataframe.py

the output will be stored in directory configured in chipalign.yml, which in this case is output/
"""
import shutil

import luigi
import pandas as pd

from chipalign.core.task import Task
from chipalign.core.util import temporary_file
from chipalign.database.encode.metadata import EncodeTFMetadata
from chipalign.signal.bins import BinnedSignal
from chipalign.signal.bins_roadmap_bowtie import BinnedSignalRoadmapBowtie

MIN_READ_LENGTH = 36

INTERESTING_CELL_TYPES = ['IMR-90', 'H1-hESC', 'H9', 'HepG2', 'K562']
INTERESTING_TRACKS = [
     #'ASH2L', 'ATF2', 'ATF3', 'BACH1', 'BCL11A', 'BHLHE40',
     'BRCA1', 'CBX5', 'CBX8', 'CEBPB',
     'CHD1',
     'CHD2', 'CHD7',
     # 'CREB1', 'CTBP2',
     'CTCF', 'E2F6', 'EGR1', 'ELK1', 'EP300',
     #'EZH2',
     'FOS', 'FOSL1',
     #'GABPA', 'GTF2F1',
     'H2AFZ',
     'H2AK5ac',
     'H2AK9ac', 'H2BK120ac',
     'H2BK12ac', 'H2BK15ac', 'H2BK20ac', 'H2BK5ac', 'H3K14ac', 'H3K18ac', 'H3K23ac', 'H3K23me2',
     'H3K27ac', 'H3K27me3', 'H3K36me3', 'H3K4ac', 'H3K4me1', 'H3K4me2', 'H3K4me3', 'H3K56ac',
     'H3K79me1', 'H3K79me2', 'H3K9ac', 'H3K9me1', 'H3K9me3', 'H3T11ph', 'H4K20me1', 'H4K5ac',
     'H4K8ac', 'H4K91ac',
     'HDAC2', 'HDAC6',
     'JUN', 'JUND', 'KDM1A', 'KDM4A', 'KDM5A',
     'KMT2B',
     # 'MAFK', 'MAX',
     #'MAZ', 'MXI1', 'MYC', 'NANOG', 'NFE2L2', 'NRF1', 'PHF8',
     'POLR2A', 'POLR2AphosphoS5',
     #'POU5F1',
     #'RAD21', 'RBBP5', 'RCOR1', 'REST', 'RFX5', 'RNF2', 'RXRA', 'SAP30',
     'SIN3A',
     #'SIRT6', 'SIX5',
     #'SMC3', 'SP1', 'SP2', 'SP4', 'SRF', 'SUZ12', 'TAF1', 'TAF7', 'TBP', 'TCF12', 'TEAD4', 'USF1',
     #'USF2',
     'YY1',
     'ZFX',
     #'ZNF143', 'ZNF274'
]

ADDITIONAL_TARGETS = {

    'H9': {
        # -- https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE60171 ---
        # 	Human ESCs BRD4 Vehicle- treated 6h (rep1+rep2)
        'BRD4': [
            ('sra', 'SRR1537736'),
            ('sra', 'SRR1537737')
        ],
        # Human ESCs BRD4 MS417- treated 6h (rep1+rep2)
        'BRD4-MS417': [
            ('sra', 'SRR1537738'),
            ('sra', 'SRR1537739'),
        ],
        #  Human ESCs BRD2 Vehicle- treated 6h
        'BRD2': [
            ('sra', 'SRR1537740'),
        ],
        # Human ESCs BRD2 MS417- treated 6h
        'BRD2-MS417': [
            ('sra', 'SRR1537741'),
        ],
        # Human ESCs BRD3 Vehicle- treated 6h
        'BRD3': [
            ('sra', 'SRR1537742'),
        ],
        # Human ESCs BRD3 MS417- treated 6h
        'BRD3-MS417': [
            ('sra', 'SRR1537743')
        ],
        # Human ESCs PolII Vehicle- treated 6h
        'PolII': [
            ('sra', 'SRR1537744')
        ],
        # Human ESCs PolII MS417- treated 6h
        'PolII-MS417': [
            ('sra', 'SRR1537745')
        ]
    },
    'IMR-90': {

        # https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE74238

        # Assuming proliferating matches encode the best.
        'H3K27ac': [
            # https://www.ncbi.nlm.nih.gov/sra?term=SRX1360979
            ('sra', 'SRR2748694'),
            # H3K27ac ChIP-Seq_Proliferating IMR90 replicate
            ('sra', 'SRR3287543'),
        ],

        'BRD4': [
            # BRD4 ChIP-Seq_Proliferating IMR90
            ('sra', 'SRR2748697'),
            # BRD4 ChIP-Seq_Proliferating IMR90 replicate
            ('sra', 'SRR3287546')
        ]
    }
}

ADDITIONAL_INPUTS = {
    'IMR-90': [
        # https://www.ncbi.nlm.nih.gov/sra?term=SRX1360985
        ('sra', 'SRR2748700'),
        # https://www.ncbi.nlm.nih.gov/sra?term=SRX1658070
        ('sra', 'SRR3287549')
    ],

    'H9': [
        # https://www.ncbi.nlm.nih.gov/sra?term=SRX670811
        ('sra', 'SRR1537734')
    ]
}


class TFSignalDataFrame(Task):
    # Since this task has dynamic dependancies we cannot run it on OGS

    run_locally = True
    # We take two parameters: genome version, and binning method
    genome_version = luigi.Parameter(default='hg38')
    binning_method = BinnedSignal.binning_method

    # One could put the interesting TFs here as well, but I am keeping it as a constant in code for
    # for simplicity

    # You need to specify this as code doesn't do that automatically
    @property
    def _extension(self):
        return 'h5'

    # Helper tasks
    @property
    def metadata_task(self):
        return EncodeTFMetadata(genome_version=self.genome_version)

    def additional_targets(self):
        """
        Override this function in subclass to inject other TFs that might be potentially interesting
        :return:
        """
        return ADDITIONAL_TARGETS

    def additional_inputs(self):
        """
        Override this function in subclass to inject other input
        files from experiments in `additional_targets` and `additional_histones`
        :return:
        """
        return ADDITIONAL_INPUTS

    def requires(self):
        # Normally one would list all the requirements here, but we do not really know
        # them until we download the metadata, thus we will have to dynamically add
        # them in run() method
        return self.metadata_task

    def _load_metadata(self):
        metadata = pd.read_csv(self.metadata_task.output().path)
        # Remove rows for which we have no data
        metadata = metadata.dropna(subset=['Biosample term name', 'target', 'File accession'])
        metadata = metadata[(metadata[['Biosample term name', 'target', 'File accession']] != '').all(axis=1)]

        # Only keep raw reads
        metadata = metadata[metadata['Output type'] == 'reads']
        # Only keep reads with length >= min length
        metadata = metadata[metadata['Read length'] >= MIN_READ_LENGTH]

        # Only keep single-ended reads (we do not support paired ends yet)
        metadata = metadata[metadata['Run type'] == 'single-ended']

        # Ensure file status is released
        metadata = metadata[metadata['File Status'] == 'released']

        # Now leave only interesting cell lines
        metadata = metadata[metadata['Biosample term name'].isin(INTERESTING_CELL_TYPES)]

        treatment_metadata = metadata[metadata['target'].isin(INTERESTING_TRACKS)]
        input_metadata = metadata[metadata['is_input']]

        logger = self.logger()
        logger.info('len(treatment_metadata): {:,}, len(input_metadata): {:,}'.format(len(treatment_metadata),
                                                                                      len(input_metadata)))

        return treatment_metadata, input_metadata


    def _compile_and_write_df(self, binned_signal_tasks, output_store, output_store_key):
        """
        Compiles dataframe from the signal tasks list and immediatelly dumps it to store

        :param binned_signal_tasks:
        :param output_store:
        :param output_store_key:
        :return:
        """
        df = {}
        for key, task in binned_signal_tasks.items():
            df[key] = task.output().load()

        df = pd.DataFrame(df)
        df.sort_index(axis=0, inplace=True)
        df.sort_index(axis=1, inplace=True)

        output_store[output_store_key] = df


    def _run(self):
        from chipalign.command_line_applications.tables import ptrepack

        # Get the logger which we will use to output current progress to terminal
        logger = self.logger()
        logger.info('Starting signal dataframe')
        logger.debug('Interesting tracks are: {!r}'.format(INTERESTING_TRACKS))

        logger.info('Loading metadata')
        target_metadata, input_metadata = self._load_metadata()

        assert len(input_metadata) > 0

        cell_types, input_accessions = self._parse_metadata(target_metadata, input_metadata)

        for cell_type, cell_additional_inputs in self.additional_inputs().items():
            input_accessions[cell_type].extend(cell_additional_inputs)

        # We now need to create the track tasks
        track_accessions = {}
        for cell_type in cell_types:
            cell_tf_accessions = {}
            cell_metadata = target_metadata[target_metadata['Biosample term name'] == cell_type]
            unique_targets_for_cell = cell_metadata['target'].unique()
            for target in unique_targets_for_cell:
                accessions = [('encode', x) for x in cell_metadata.query('target == @target')['File accession'].unique()]

                cell_tf_accessions[target] = accessions

            track_accessions[cell_type] = cell_tf_accessions

        for cell_type, cell_additional_tfs in self.additional_targets().items():
            if cell_type not in track_accessions:
                track_accessions[cell_type] = {}

            for target, additional_accessions in cell_additional_tfs.items():
                try:
                    track_accessions[cell_type][target].extend(additional_accessions)
                except KeyError:
                    track_accessions[cell_type][target] = additional_accessions

        logger.debug('Got {:,} TF tasks'.format(sum(map(len, track_accessions.values()))))

        track_tasks = {}
        for cell_type in cell_types:
            input_accessions_str = ';'.join(['{}:{}'.format(*x) for x in input_accessions[cell_type]])

            ct_track_tasks = {}

            for target, accessions in track_accessions[cell_type].items():
                accessions_str = ';'.join(['{}:{}'.format(*x) for x in accessions])
                logger.debug(f'Cell type: {cell_type!r}, input_accessions: {input_accessions_str!r}, target_accessions: {accessions_str!r}')

                ct_track_tasks[target] = BinnedSignalRoadmapBowtie(genome_version=self.genome_version,
                                                                   cell_type=cell_type,
                                                                   binning_method=self.binning_method,
                                                                   treatment_accessions_str=accessions_str,
                                                                   input_accessions_str=input_accessions_str)

            track_tasks[cell_type] = ct_track_tasks

        # Make sure to yield the tasks at the same time:
        joint = []
        for cell_type in cell_types:
            joint.extend(track_tasks[cell_type].values())

        logger.info('Yielding {:,} new tasks'.format(len(joint)))

        # Note that luigi does not play well when you yield tasks that take other tasks as input
        # it is one of the reasons why we use _*BinnedSignal metatasks above
        yield joint

        # If we are at this stage, we have all the data we need,
        # only left to combine it to dataframes
        # in order to keep memory footprint low, we're going to write them as we go
        # in case program gets terminated on the way, we do this to a temp file first

        with temporary_file() as temp_filename:
            with pd.HDFStore(temp_filename, 'w') as store:
                for cell_type in cell_types:
                    ts = track_tasks[cell_type]

                    logger.info('Compiling dataframe for {}'.format(cell_type))
                    self._compile_and_write_df(ts, store, 'tracks/{}'.format(cell_type))

                store['/target_metadata'] = target_metadata
                store['/input_metadata'] = input_metadata

            # Nearly done
            logger.info('Compressing output')
            with temporary_file() as compressed_temp_filename:
                ptrepack('--chunkshape', 'auto',
                         '--propindexes',
                         '--complevel', 1,
                         '--complib', 'lzo',
                         temp_filename, compressed_temp_filename
                         )

                logger.info('Copying output')
                self.ensure_output_directory_exists()
                shutil.move(compressed_temp_filename, self.output().path)

    def _parse_metadata(self, metadata, input_metadata):
        logger = self.logger()

        cell_types = list(metadata['Biosample term name'].unique())
        cell_types = cell_types + INTERESTING_CELL_TYPES
        input_accessions = {}
        for cell_type in cell_types:
            input_accessions[cell_type] = [('encode', x)
                                           for x in input_metadata.loc[input_metadata['Biosample term name'] == cell_type, 'File accession'].unique()]

            logger.debug('Input accessions for {}: {!r}'.format(cell_type,
                                                                input_accessions[cell_type]))
        logger.info(
            'Found {:,} cell types that contain the interesting tracks'.format(len(cell_types)))
        logger.debug('Cell types found: {!r}'.format(sorted(list(cell_types))))
        found_tfs = metadata['target'].value_counts()
        logger.debug('TFs found: {}'.format(found_tfs))
        logger.debug('Number of datasets found: {:,}'.format(len(metadata)))
        return cell_types, input_accessions


if __name__ == '__main__':
    luigi.run(main_task_cls=TFSignalDataFrame)
