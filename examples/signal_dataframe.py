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

from chipalign.alignment.consolidation import ConsolidatedReads
from chipalign.alignment.filtering import FilteredReads
from chipalign.core.file_formats.dataframe import DataFrameFile
from chipalign.core.task import Task, MetaTask
from chipalign.core.util import temporary_file
from chipalign.database.encode.download import EncodeDownloadedSignal, EncodeAlignedReads
from chipalign.database.encode.metadata import EncodeTFMetadata
import pandas as pd
import luigi

from chipalign.database.roadmap.downloaded_filtered_reads import RoadmapDownloadedFilteredReads
from chipalign.database.roadmap.mappable_bins import RoadmapMappableBins
from chipalign.database.roadmap.metadata import roadmap_targets_for_cell_line, \
    roadmap_consolidated_read_download_uris
from chipalign.signal.bins import BinnedSignal
from chipalign.signal.signal import Signal

# INTERESTING_TFS = ['BPTF', 'PHF2', 'YNG1', 'ING4', 'TAF3', 'RAG2', 'PYGO', 'MLL1', 'JARID1A', 'BHC80',
#                    'AIRE', 'DNMT3L', 'TRIM24', 'DPF3B', 'PHF19', 'GCN5', 'BMI1',
#                    'PRC1', 'PRC2', 'RING1', 'RING2', 'JMJD5', 'JMJD6',
#                    'UTX', 'UTY', 'JMJD3', 'CHD1', 'CHD2', 'CBX1',
#                    'CBX2', 'CBX3', 'CBX4', 'CBX5', 'CBX6', 'CBX7', 'CBX8']

INTERESTING_TFS = ['CHD1']
INTERESTING_CELL_TYPES = ['E003', 'E017']


class _ConsolidatedInputs(MetaTask):

    cell_type = RoadmapMappableBins.cell_type
    genome_version = RoadmapMappableBins.genome_version
    encode_accessions_str = luigi.Parameter()

    _parameter_names_to_hash = ('encode_accessions_str', )

    def _roadmap_filtered_inputs(self):
        uris = roadmap_consolidated_read_download_uris(self.cell_type, 'Input')
        filtered_read_tasks = [RoadmapDownloadedFilteredReads(uri=uri,
                                                              genome_version=self.genome_version) for uri in uris]
        return filtered_read_tasks

    @property
    def metadata_task(self):
        return EncodeTFMetadata(genome_version=self.genome_version)

    def _encode_filtered_inputs(self):
        ans = []
        # passing as string works around luigis problems with serialising
        encode_accessions = self.encode_accessions_str.split(';')

        for accession in encode_accessions:
            aligned_reads = EncodeAlignedReads(accession=accession)
            filtered_reads = FilteredReads(genome_version=self.genome_version,
                                           alignment_task=aligned_reads)

            ans.append(filtered_reads)

        return ans

    def requires(self):
        subtasks = self._roadmap_filtered_inputs()
        subtasks.extend(self._encode_filtered_inputs())

        return ConsolidatedReads(input_alignments=subtasks)


class _BinnedSignalMeta(MetaTask):
    """
    A metaclass that creates appropriate binned signal task given the signal task function
    """
    genome_version = RoadmapMappableBins.genome_version
    cell_type = RoadmapMappableBins.cell_type
    binning_method = BinnedSignal.binning_method

    encode_input_accessions_str = luigi.Parameter()

    def input_task(self):
        return _ConsolidatedInputs(cell_type=self.cell_type,
                                   genome_version=self.genome_version,
                                   encode_accessions_str=self.encode_input_accessions_str)

    def treatment_task(self):
        raise NotImplementedError

    def signal_task(self):
        return Signal(input_task=self.input_task(),
                      treatment_task=self.treatment_task())

    def bins_task(self):
        return RoadmapMappableBins(genome_version=self.genome_version,
                                   cell_type=self.cell_type)

    def binned_signal(self):
        bins = self.bins_task()
        signal = self.signal_task()

        binned_signal = BinnedSignal(bins_task=bins,
                                     signal_task=signal,
                                     binning_method=self.binning_method
                                     )
        return binned_signal

    def requires(self):
        return self.binned_signal()


class _RoadmapBinnedSignal(_BinnedSignalMeta):
    track = luigi.Parameter()

    def treatment_task(self):
        uris = roadmap_consolidated_read_download_uris(self.cell_type,
                                                       self.track)

        filtered = [RoadmapDownloadedFilteredReads(uri=uri,
                                                   genome_version=self.genome_version) for uri in uris]
        return ConsolidatedReads(input_alignments=filtered)


class _EncodeBinnedSignal(_BinnedSignalMeta):
    target_accessions_str = luigi.Parameter()

    def treatment_task(self):
        filtered = []

        # luigi will automatically do a repr on the list if scheduled by yield
        # work around this by pushing it as a semi-colon separated string
        target_accessions = self.target_accessions_str.split(';')

        for accession in target_accessions:
            aligned_reads = EncodeAlignedReads(accession=accession)
            filtered_reads = FilteredReads(genome_version=self.genome_version,
                                           alignment_task=aligned_reads)

            filtered.append(filtered_reads)

        return ConsolidatedReads(input_alignments=filtered)

class TFSignalDataFrame(Task):

    # We take two parameters: genome version, and binning method
    genome_version = luigi.Parameter(default='hg19')
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

    def requires(self):
        # Normally one would list all the requirements here, but we do not really know
        # them until we download the metadata, thus we will have to dynamically add
        # them in run() method
        return self.metadata_task

    def _load_metadata(self):
        metadata = pd.read_csv(self.metadata_task.output().path)
        # Remove cell lines for which we have no roadmap data for
        metadata = metadata.dropna(subset=['roadmap_cell_type'])
        # Only keep aligned reads
        metadata = metadata[metadata['Output type'] == 'alignments']
        # Remove all targets that are not interesting
        # the replace command gets rid of the eGFP- and FLAG- bits
        # which indicate tags used to pull down
        tf_metadata = metadata[metadata['target'].str.replace('^eGFP-|FLAG-', '').isin(INTERESTING_TFS)]

        logger = self.logger()
        biosamples = tf_metadata['Biosample term id'].unique()
        logger.debug('Biosample ids found: {!r}'.format(biosamples))

        input_metadata = metadata[metadata['Biosample term id'].isin(biosamples) & metadata['is_input']]

        return tf_metadata, input_metadata

    def _roadmap_histone_target_list(self, cell_type):
        """
        :param cell_type:
        :return:
        """
        return roadmap_targets_for_cell_line(cell_type)

    def _load_histone_signal_tasks(self, cell_type, targets, input_accessions):
        """
        Loads signal tasks from SignalTrackList task
        :param cell_type: cell type to use
        :param target: targets
        :param input_accessions: ENCODE accessions for the inputs from encode datasets
        :return:
        """
        ans = {}
        for track in targets:
            signal = _RoadmapBinnedSignal(genome_version=self.genome_version,
                                          cell_type=cell_type,
                                          track=track,
                                          binning_method=self.binning_method,
                                          encode_input_accessions_str=';'.join(input_accessions))

            ans[track] = signal
        return ans

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


    def run(self):
        from chipalign.command_line_applications.tables import ptrepack

        # Get the logger which we will use to output current progress to terminal
        logger = self.logger()
        logger.info('Starting signal dataframe')
        logger.debug('Interesting TFs are: {!r}'.format(INTERESTING_TFS))

        logger.info('Loading metadata')
        metadata, input_metadata = self._load_metadata()

        assert len(input_metadata) > 0

        input_accessions = input_metadata['File accession'].unique()

        logger.debug('Input accessions: {!r}'.format(input_accessions))
        cell_types = list(metadata['roadmap_cell_type'].unique())
        cell_types = cell_types

        logger.info('Found {:,} cell types that contain the interesting TFs'.format(len(cell_types)))
        logger.debug('Cell types found: {!r}'.format(sorted(list(cell_types))))

        found_tfs = metadata['target'].value_counts()
        logger.debug('TFs found: {}'.format(found_tfs))

        logger.debug('Number of datasets found: {:,}'.format(len(metadata)))

        logger.info('Fetching available signals from roadmap')

        # Once we know the cell lines we can fetch the tracklist from roadmap
        roadmap_histone_targets = {cell_type: self._roadmap_histone_target_list(cell_type)
                                   for cell_type in cell_types}

        # At this point we have response so we can create the histone tasks directly
        histone_signals = {}
        for cell_type, targets in roadmap_histone_targets.items():
            histone_signals[cell_type] = self._load_histone_signal_tasks(cell_type,
                                                                         targets, input_accessions)

        logger.debug('Got {:,} histone tasks'.format(sum(map(len, histone_signals.values()))))

        # We have the histone tasks, now we only need to create the TF tasks
        tf_signals = {}
        for cell_type in cell_types:
            cell_tf_signals = {}
            cell_metadata = metadata.query('roadmap_cell_type == @cell_type')
            unique_targets_for_cell = cell_metadata['target'].unique()
            for target in unique_targets_for_cell:
                accessions = cell_metadata.query('target == @target')['File accession'].unique()

                cell_tf_signals[target] = _EncodeBinnedSignal(target_accessions_str=';'.join(accessions),
                                                              cell_type=cell_type,
                                                              genome_version=self.genome_version,
                                                              binning_method=self.binning_method,
                                                              encode_input_accessions_str=';'.join(input_accessions))

            tf_signals[cell_type] = cell_tf_signals

        logger.debug('Got {:,} TF tasks'.format(sum(map(len, tf_signals.values()))))

        # Make sure to yield the tasks at the same time:
        joint = []
        for cell_type in cell_types:
            joint.extend(histone_signals[cell_type].values())
            joint.extend(tf_signals[cell_type].values())

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
                    hs = histone_signals[cell_type]
                    ts = tf_signals[cell_type]

                    logger.info('Compiling dataframe for {}'.format(cell_type))
                    self._compile_and_write_df(hs, store, 'histones/{}'.format(cell_type))
                    self._compile_and_write_df(ts, store, 'tfs/{}'.format(cell_type))

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

if __name__ == '__main__':
    luigi.run(main_task_cls=TFSignalDataFrame)
