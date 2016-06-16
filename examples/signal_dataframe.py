"""
Example on how to download Signal from Roadmap

Prior to running this example, just like all examples ensure that luigi scheduler is running,
by typing:

    luigid

You can then run this by

python signal_dataframe.py --cell_type E123

the output will be stored in directory configured in chipalign.yml, which in this case is output/
"""
import chipalign.database.roadmap_data.histone_signal_dataframe

if __name__ == '__main__':
    import luigi
    luigi.run(main_task_cls=chipalign.database.roadmap_data.histone_signal_dataframe.RoadmapHistoneSignal)
