import shutil

import luigi
from signal_dataframe import INTERESTING_TRACKS, \
    HISTONE_TRACKS, ADDITIONAL_TARGETS, ADDITIONAL_INPUTS, MIN_READ_LENGTH

from chipalign.core.file_formats.dataframe import compress_dataframe
from chipalign.core.task import Task
from chipalign.core.util import temporary_file, timed_segment
from chipalign.database.encode.metadata import EncodeTFMetadata
from chipalign.signal.bins import BinnedSignal
import pandas as pd

from chipalign.signal.matrixbins_bowtie import MatrixBinnedSignalBowtie

INTERESTING_CELL_TYPES = ['IMR-90', 'H1-hESC', 'H9']

SUBSET = {
    'ASH2L',
    'BRCA1',
    'BRD4', # Not in encode, but important control
    'CBX5',
    'CBX8',
    'CHD1',
    'CHD7',
    'CREB1',
    'CTCF',
    'EZH2',
    'HDAC2',
    'KDM1A',
    'KDM5A',
    'MAX',
    'MAZ',
    'MAZ',
    'MXI1',
    'MYC',
    'PHF8',
    'PHF8',
    'RBBP5',
    'RCOR1',
    'REST',
    'RNF2',
    'SAP30',
    'SIN3A',
    'SUZ12',
    'TAF1',
    'TAF7',
    'TBP',
    'TEAD4',
    'USF1',
    'YY1',
}

INTERESTING_TRACKS = [t for t in INTERESTING_TRACKS if t in SUBSET or t in HISTONE_TRACKS]

MAX_WINDOWS = 1000

class MatrixDataFrame(Task):
    # Since this task has dynamic dependancies we cannot run it on OGS

    run_locally = True
    # We take two parameters: genome version, and binning method
    genome_version = luigi.Parameter(default='hg38')
    binning_method = BinnedSignal.binning_method
    window_size = luigi.IntParameter(default=20)
    slop = luigi.IntParameter(default=2000)

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

    def interesting_cell_types(self):
        return INTERESTING_CELL_TYPES

    def interesting_tracks(self):
        return INTERESTING_TRACKS

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
        metadata = metadata[metadata['Biosample term name'].isin(self.interesting_cell_types())]

        treatment_metadata = metadata[metadata['target'].isin(self.interesting_tracks())]
        input_metadata = metadata[metadata['is_input']]

        logger = self.logger()
        logger.info('len(treatment_metadata): {:,}, len(input_metadata): {:,}'.format(len(treatment_metadata),
                                                                                      len(input_metadata)))

        return treatment_metadata, input_metadata

    def _run(self):

        # Get the logger which we will use to output current progress to terminal
        logger = self.logger()
        logger.info('Starting matrix dataframe')
        logger.debug('Interesting tracks are: {!r}'.format(self.interesting_tracks()))

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

            for tf_target, tf_accessions in track_accessions[cell_type].items():
                if tf_target in HISTONE_TRACKS:
                    continue
                tf_accessions_str = ';'.join(['{}:{}'.format(*x) for x in tf_accessions])

                tf_target_tasks = {}
                for other_target, accessions_other in track_accessions[cell_type].items():
                    other_accessions_str = ';'.join(['{}:{}'.format(*x) for x in accessions_other])

                    logger.debug(f'Cell type: {cell_type!r}, input_accessions: {input_accessions_str!r}, tf_accessions: {tf_accessions_str!r}, other_accessions: {other_accessions_str!r}')

                    tf_target_tasks[other_target] = MatrixBinnedSignalBowtie(genome_version=self.genome_version,
                                                                             cell_type=cell_type,
                                                                             read_length=MIN_READ_LENGTH,
                                                                             binning_method=self.binning_method,
                                                                             treatment_accessions_str=other_accessions_str,
                                                                             input_accessions_str=input_accessions_str,
                                                                             matrix_accessions_str=tf_accessions_str,
                                                                             matrix_slop=self.slop,
                                                                             matrix_window_size=self.window_size,
                                                                             matrix_limit=MAX_WINDOWS,
                                                                             )
                ct_track_tasks[tf_target] = tf_target_tasks

            track_tasks[cell_type] = ct_track_tasks

        # Make sure to yield the tasks at the same time:
        joint = []
        for cell_type in cell_types:
            for tf, subtasks in track_tasks[cell_type].items():
                joint.extend(subtasks.values())

        logger.info('Yielding {:,} new tasks'.format(len(joint)))

        # Note that luigi does not play well when you yield tasks that take other tasks as input
        # it is one of the reasons why we use _*BinnedSignal metatasks above
        yield joint

        # If we are at this stage, we have all the data we need,
        # only left to combine it to dataframes
        # in order to keep memory footprint low, we're going to write them as we go
        # in case program gets terminated on the way, we do this to a temp file first

        with temporary_file() as temp_filename:

            with timed_segment("Compiling data for {}".format(self.__class__.__name__), logger=logger):
                with pd.HDFStore(temp_filename, 'w') as store:
                    for cell_type in cell_types:
                        ts = track_tasks[cell_type]
                        for tf, tasks in ts.items():
                            for other, task in tasks.items():
                                df = task.output().load()
                                store[f'/tracks/{cell_type}/{tf}/{other}'] = df

                    store['/target_metadata'] = target_metadata
                    store['/input_metadata'] = input_metadata

            # Nearly done
            with timed_segment('Compressing data for {}'.format(self.__class__.__name__,
                                                                logger=logger)):
                with temporary_file() as compressed_temp_filename:
                    compress_dataframe(temp_filename, compressed_temp_filename)
                    self.ensure_output_directory_exists()
                    shutil.move(compressed_temp_filename, self.output().path)

    def _parse_metadata(self, metadata, input_metadata):
        logger = self.logger()

        cell_types = list(metadata['Biosample term name'].unique())
        cell_types = cell_types + self.interesting_cell_types()
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
    luigi.run(main_task_cls=MatrixDataFrame)
