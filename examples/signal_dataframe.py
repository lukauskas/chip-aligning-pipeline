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

from chipalign.alignment.aligned_reads import AlignedReads
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

INTERESTING_TFS = ['CHD1', 'CHD2', 'CBX1',
                   'CBX2', 'CBX3', 'CBX4', 'CBX5', 'CBX6', 'CBX7', 'CBX8']

# INTERESTING_TFS = ['CBX1']
# INTERESTING_CELL_TYPES = []
INTERESTING_CELL_TYPES = ['E003', 'E017', 'E008']
# There's something wrong with fragment length calculation for these, TODO: debug.
ROADMAP_BLACKLIST = [('E116', 'h3k36me3'),
                     ('E117', 'h3k36me3')]

class _FilteredReads(MetaTask):

    genome_version = RoadmapMappableBins.genome_version
    accession = luigi.Parameter()
    source = luigi.Parameter()

    def _encode_task(self):
        aligned_reads = EncodeAlignedReads(accession=self.accession)
        filtered_reads = FilteredReads(genome_version=self.genome_version,
                                       alignment_task=aligned_reads)
        return filtered_reads

    def _roadmap_task(self):
        return RoadmapDownloadedFilteredReads(uri=self.accession,
                                              genome_version=self.genome_version)

    def _sra_task(self):
        aligned_reads = AlignedReads(genome_version=self.genome_version,
                                     accession=self.accession,
                                     source=self.source,
                                     aligner='bowtie'
                                     )

        filtered_reads = FilteredReads(genome_version=self.genome_version,
                                       alignment_task=aligned_reads)

        return filtered_reads

    def requires(self):
        if self.source == 'encode':
            return self._encode_task()
        elif self.source == 'roadmap':
            return self._roadmap_task()
        elif self.source == 'sra':
            return self._sra_task()
        else:
            raise ValueError('Unsupported source: {!r}'.format(self.source))

class _ConsolidatedReads(MetaTask):
    _parameter_names_to_hash = ('accessions_str',)

    genome_version = RoadmapMappableBins.genome_version
    accessions_str = luigi.Parameter()
    cell_type = luigi.Parameter()

    def requires(self):
        accessions = self.accessions_str.split(';')
        filtered = []

        for source_accession in accessions:
            source, __, accession = source_accession.partition(':')

            if source == 'roadmap':
                uris = roadmap_consolidated_read_download_uris(self.cell_type,
                                                               accession)
                for uri in uris:
                    filtered.append(_FilteredReads(source='roadmap',
                                                   accession=uri,
                                                   genome_version=self.genome_version))
            else:
                filtered.append(_FilteredReads(source=source, accession=accession,
                                               genome_version=self.genome_version))

        return ConsolidatedReads(input_alignments=filtered)

class _BinnedSignal(MetaTask):
    """
    A metaclass that creates appropriate binned signal task given the signal task function
    """
    genome_version = RoadmapMappableBins.genome_version
    cell_type = RoadmapMappableBins.cell_type
    binning_method = BinnedSignal.binning_method

    treatment_accessions_str = luigi.Parameter()
    input_accessions_str = luigi.Parameter()

    def input_task(self):
        return _ConsolidatedReads(cell_type=self.cell_type,
                                  genome_version=self.genome_version,
                                  accessions_str=self.input_accessions_str)

    def treatment_task(self):
        return _ConsolidatedReads(accessions_str=self.treatment_accessions_str,
                                  genome_version=self.genome_version,
                                  cell_type=self.cell_type)

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

    def additional_tfs(self):
        return {}

    def additional_inputs(self):
        return {}

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
        tracks = roadmap_targets_for_cell_line(cell_type)
        tracks = [track for track in tracks if (cell_type, track.lower()) not in ROADMAP_BLACKLIST]
        return tracks


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

        cell_types, input_accessions = self._parse_metadata(metadata, input_metadata)

        for cell_type in cell_types:
            try:
                input_accessions[cell_type].append(('roadmap', 'Input'))
            except KeyError:
                input_accessions[cell_type] = [('roadmap', 'Input')]

        histone_accessions = self._get_roadmap_histone_accessions(cell_types)

        # We have the histone tasks, now we only need to create the TF tasks
        tf_accessions = {}
        for cell_type in cell_types:
            cell_tf_accessions = {}
            cell_metadata = metadata.query('roadmap_cell_type == @cell_type')
            unique_targets_for_cell = cell_metadata['target'].unique()
            for target in unique_targets_for_cell:
                accessions = [('encode', x) for x in cell_metadata.query('target == @target')['File accession'].unique()]

                cell_tf_accessions[target] = accessions

            tf_accessions[cell_type] = cell_tf_accessions

        logger.debug('Got {:,} TF tasks'.format(sum(map(len, tf_accessions.values()))))

        histone_tasks = {}
        tf_tasks = {}
        for cell_type in cell_types:
            ct_histone_tasks = {}
            input_accessions_str = ';'.join(['{}:{}'.format(*x) for x in input_accessions[cell_type]])
            for target, accessions in histone_accessions[cell_type].items():
                accessions_str = ';'.join(['{}:{}'.format(*x) for x in accessions])

                ct_histone_tasks[target] = _BinnedSignal(genome_version=self.genome_version,
                                                         cell_type=cell_type,
                                                         binning_method=self.binning_method,
                                                         treatment_accessions_str=accessions_str,
                                                         input_accessions_str=input_accessions_str)
            histone_tasks[cell_type] = ct_histone_tasks

            ct_tf_tasks = {}

            for target, accessions in tf_accessions[cell_type].items():
                accessions_str = ';'.join(['{}:{}'.format(*x) for x in accessions])

                ct_tf_tasks[target] = _BinnedSignal(genome_version=self.genome_version,
                                                    cell_type=cell_type,
                                                    binning_method=self.binning_method,
                                                    treatment_accessions_str=accessions_str,
                                                    input_accessions_str=input_accessions_str)

            tf_tasks[cell_type] = ct_tf_tasks

        # Make sure to yield the tasks at the same time:
        joint = []
        for cell_type in cell_types:
            joint.extend(histone_tasks[cell_type].values())
            joint.extend(tf_tasks[cell_type].values())

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
                    hs = histone_tasks[cell_type]
                    ts = tf_tasks[cell_type]

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

    def _parse_metadata(self, metadata, input_metadata):
        logger = self.logger()

        cell_types = list(metadata['roadmap_cell_type'].unique())
        cell_types = cell_types + INTERESTING_CELL_TYPES
        input_accessions = {}
        for cell_type in cell_types:
            input_accessions[cell_type] = [('encode', x)
                                           for x in input_metadata.query('roadmap_cell_type == @cell_type')['File accession'].unique()]

            logger.debug('Input accessions for {}: {!r}'.format(cell_type,
                                                                input_accessions[cell_type]))
        logger.info(
            'Found {:,} cell types that contain the interesting TFs'.format(len(cell_types)))
        logger.debug('Cell types found: {!r}'.format(sorted(list(cell_types))))
        found_tfs = metadata['target'].value_counts()
        logger.debug('TFs found: {}'.format(found_tfs))
        logger.debug('Number of datasets found: {:,}'.format(len(metadata)))
        return cell_types, input_accessions

    def _get_roadmap_histone_accessions(self, cell_types):
        logger = self.logger()
        logger.info('Fetching available signals from roadmap')
        # Once we know the cell lines we can fetch the track list from roadmap
        roadmap_histone_targets = {cell_type: self._roadmap_histone_target_list(cell_type)
                                   for cell_type in cell_types}

        histone_accessions = {}
        for cell_type, targets in roadmap_histone_targets.items():
            histone_accessions[cell_type] = {target: [('roadmap', target)] for target in targets}

        logger.debug('Got {:,} histone tasks'.format(sum(map(len, histone_accessions.values()))))
        return histone_accessions


if __name__ == '__main__':
    luigi.run(main_task_cls=TFSignalDataFrame)
