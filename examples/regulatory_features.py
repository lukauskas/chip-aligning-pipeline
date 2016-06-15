"""
Example on how to download Signal from Roadmap

Prior to running this example, just like all examples ensure that luigi scheduler is running,
by typing:

    luigid

You can then run this by

python regulatory_features.py

the output will be stored in directory configured in chipalign.yml, which in this case is output/
"""

import luigi

from chipalign.core.task import MetaTask
from chipalign.biomart.regulatory_features import RegulatoryFeatures


class ExampleRegulatoryFeatures(MetaTask):
    def requires(self):
        return RegulatoryFeatures(genome_version='hg19')

if __name__ == '__main__':
    luigi.run(main_task_cls=ExampleRegulatoryFeatures)
