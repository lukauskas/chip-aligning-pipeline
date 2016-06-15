import chipalign.roadmap_data.histone_signal_dataframe


if __name__ == '__main__':
    import luigi
    luigi.run(main_task_cls=chipalign.roadmap_data.histone_signal_dataframe.RoadmapHistoneSignal)
