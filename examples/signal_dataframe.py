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

from chipalign.core.file_formats.dataframe import DataFrameFile
from chipalign.core.task import Task, MetaTask
from chipalign.core.util import temporary_file
from chipalign.database.encode.download import EncodeDownloadedSignal
from chipalign.database.encode.metadata import EncodeTFMetadata
import pandas as pd
import luigi

from chipalign.database.roadmap.downloaded_signal import RoadmapDownloadedSignal
from chipalign.database.roadmap.mappable_bins import RoadmapMappableBins
from chipalign.database.roadmap.signal_tracks_list import SignalTracksList
from chipalign.signal.bins import BinnedSignal

INTERESTING_TFS = ['BPTF', 'PHF2', 'YNG1', 'ING4', 'TAF3', 'RAG2', 'PYGO', 'MLL1', 'JARID1A', 'BHC80', 'AIRE', 'DNMT3L', 'TRIM24', 'DPF3B', 'PHF19', 'GCN5', 'BMI1', 'PRC1', 'PRC2', 'RING1', 'RING2', 'JMJD5', 'JMJD6', 'UTX', 'UTY', 'JMJD3', 'CHD1', 'CHD2', 'CBX1', 'CBX2', 'CBX3', 'CBX4', 'CBX5', 'CBX6', 'CBX7', 'CBX8']

INTERESTING_CELL_TYPES = ['E003', 'E017']

class _BinnedSignalMeta(MetaTask):
    """
    A metaclass that creates appropriate binned signal task given the signal task function
    """
    genome_version = RoadmapMappableBins.genome_version
    cell_type = RoadmapMappableBins.cell_type
    binning_method = BinnedSignal.binning_method

    def signal_task(self):
        raise NotImplementedError

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

    def signal_task(self):
        return RoadmapDownloadedSignal(genome_version=self.genome_version,
                                       cell_type=self.cell_type,
                                       track=self.track)


class _EncodeBinnedSignal(_BinnedSignalMeta):
    accession = luigi.Parameter()

    def signal_task(self):
        return EncodeDownloadedSignal(accession=self.accession)

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
        # Drop all files that are non-signal-p-values
        metadata = metadata[metadata['Output type'] == 'signal p-value']
        # Remove all targets that are not interesting
        # the replace command gets rid of the eGFP- and FLAG- bits
        # which indicate tags used to pull down
        metadata = metadata[metadata['target'].str.replace('^eGFP-|FLAG-', '').isin(INTERESTING_TFS)]

        # Get only the files that contain pooled data from multiple experiments
        pooled_data = []
        for experiment, group_ in metadata.groupby('Experiment accession'):
            row = group_.loc[group_['n_replicates'].argmax()]
            pooled_data.append(row)

        return pd.DataFrame(pooled_data)

    def _signal_track_list_task(self, cell_type):
        """
        Creates signal track list task for cell type
        :param cell_type:
        :return:
        """
        return SignalTracksList(genome_version=self.genome_version,
                                cell_type=cell_type)

    def _load_signal_tasks(self, signal_track_list_task):
        """
        Loads signal tasks from SignalTrackList task
        :param signal_track_list_task:
        :return:
        """
        tracks = signal_track_list_task.output().load()
        ans = {}
        for track in tracks:
            signal = _RoadmapBinnedSignal(genome_version=signal_track_list_task.genome_version,
                                          cell_type=signal_track_list_task.cell_type,
                                          track=track,
                                          binning_method=self.binning_method)

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
        metadata = self._load_metadata()

        cell_types = list(metadata['roadmap_cell_type'].unique())
	cell_types = cell_types + INTERESTING_CELL_TYPES

        logger.info('Found {:,} cell types that contain the interesting TFs'.format(len(cell_types)))
        logger.debug('Cell types found: {!r}'.format(sorted(list(cell_types))))

	found_tfs = metadata['target'].value_counts()
	logger.debug('TFs found: {}'.format(found_tfs))

        # The code below assumes only one dataset per cell line.
        # Therefore lets make this explicit and fail if there are more.
        # Particularly, there are five TFs at the point of writing this that don't satisfy this
        # condition
        assert (metadata.groupby(['roadmap_cell_type', 'target']).count()['File format'] == 1).all()

        logger.debug('Number of datasets found: {:,}'.format(len(metadata)))

        logger.info('Fetching available signals from roadmap')

        # Once we know the cell lines we can fetch the tracklist from roadmap
        downloadable_signal_tasks = [self._signal_track_list_task(cell_type)
                                     for cell_type in cell_types]

        # this tells luigi that we need to wait for these tasks to finish before we can continue
        yield downloadable_signal_tasks

        # At this point we have response so we can create the histone tasks directly
        histone_signals = {}
        for dst in downloadable_signal_tasks:
            histone_signals[dst.cell_type] = self._load_signal_tasks(dst)

        logger.debug('Got {:,} histone tasks'.format(sum(map(len, histone_signals.values()))))

        # We have the histone tasks, now we only need to create the TF tasks
        tf_signals = {}
        for cell_type in cell_types:
            cell_tf_signals = {}
            cell_metadata = metadata.query('roadmap_cell_type == @cell_type')
            for ix, row in cell_metadata.iterrows():
                target = row['target']
                accession = row['File accession']
                cell_tf_signals[target] = _EncodeBinnedSignal(accession=accession,
                                                              cell_type=cell_type,
                                                              genome_version=self.genome_version,
                                                              binning_method=self.binning_method)

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
