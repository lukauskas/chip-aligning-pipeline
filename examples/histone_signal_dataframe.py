from signal_dataframe import TFSignalDataFrame, HISTONE_TRACKS
import luigi


class HistoneSignalDataFrame(TFSignalDataFrame):

    def interesting_tracks(self):
        return HISTONE_TRACKS

if __name__ == '__main__':
    luigi.run(main_task_cls=HistoneSignalDataFrame)
