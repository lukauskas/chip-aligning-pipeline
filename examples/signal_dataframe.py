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

from chipalign.alignment import AlignedReadsBwa
from chipalign.alignment.consolidation import ConsolidatedReads
from chipalign.alignment.filtering import FilteredReads
from chipalign.core.task import Task, MetaTask
from chipalign.core.util import temporary_file
from chipalign.database.encode.metadata import EncodeTFMetadata
from chipalign.database.roadmap.mappable_bins import RoadmapMappableBins
from chipalign.database.roadmap.metadata import roadmap_consolidated_read_download_uris
from chipalign.signal.bins import BinnedSignal
from chipalign.signal.signal import Signal

MIN_READ_LENGTH = 36

INTERESTING_CELL_TYPES = ['IMR-90']
INTERESTING_TRACKS = [
    'H3K14ac',
    'CHD1'
]


class _FilteredReads(MetaTask):

    genome_version = RoadmapMappableBins.genome_version
    accession = luigi.Parameter()
    source = luigi.Parameter()

    def _aligned_task(self):

        aligned_reads = AlignedReadsBwa(genome_version=self.genome_version,
                                        accession=self.accession,
                                        source=self.source)

        filtered_reads = FilteredReads(genome_version=self.genome_version,
                                       alignment_task=aligned_reads)
        return filtered_reads

    def requires(self):
        return self._aligned_task()


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

    def additional_tfs(self):
        """
        Override this function in subclass to inject other TFs that might be potentially interesting
        :return:
        """
        return {}

    def additional_inputs(self):
        """
        Override this function in subclass to inject other input
        files from experiments in `additional_tfs` and `additional_histones`
        :return:
        """
        return {}

    def additional_histones(self):
        """
        Override this function in subclass to inject other histone signals that might be potentially interesting
        :return:
        """
        return {}

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

        for cell_type, cell_additional_tfs in self.additional_tfs().items():
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

                ct_track_tasks[target] = _BinnedSignal(genome_version=self.genome_version,
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
