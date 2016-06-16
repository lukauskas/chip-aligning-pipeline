"""
Example on how to download Signal from Roadmap

Prior to running this example, just like all examples ensure that luigi scheduler is running,
by typing:

    luigid

You can then run this by

python signal_dataframe.py --cell_type E123

the output will be stored in directory configured in chipalign.yml, which in this case is output/
"""
from chipalign.database.roadmap.histone_signal_dataframe import RoadmapHistoneSignal

if __name__ == '__main__':
    import luigi
    luigi.run(main_task_cls=RoadmapHistoneSignal)
