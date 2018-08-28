from signal_dataframe import TFSignalDataFrame, HISTONE_TRACKS
import luigi
import pandas as pd


class SignalDataFrameMeta(TFSignalDataFrame):

    @property
    def _extension(self):
        return 'bed'

    def _run(self):
        from chipalign.command_line_applications.tables import ptrepack

        # Get the logger which we will use to output current progress to terminal
        logger = self.logger()
        logger.info('Starting signal dataframe metadata')
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
                accessions = [('encode', x) for x in cell_metadata.query('target == @target')[
                    'File accession'].unique()]

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

        ans_df = []

        for cell_type in cell_types:
            input_accessions_str = '; '.join(
                ['{}:{}'.format(*x) for x in input_accessions[cell_type]])

            ans_df.append([cell_type, 'Input', input_accessions_str])

            for target, accessions in track_accessions[cell_type].items():
                accessions_str = '; '.join(['{}:{}'.format(*x) for x in accessions])

                ans_df.append([cell_type, target, accessions_str])

        ans_df = pd.DataFrame(ans_df, columns=['Cell type', 'Target', 'Accessions']).sort_values(by=['Cell type', 'Target'])
        ans_df.to_csv(self.output().path, index=False)

if __name__ == '__main__':
    luigi.run(main_task_cls=SignalDataFrameMeta)
