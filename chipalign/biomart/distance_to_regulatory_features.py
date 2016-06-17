from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import luigi
import os
import pandas as pd

from chipalign.biomart.regulatory_features import RegulatoryFeatures
from chipalign.core.file_formats.dataframe import DataFrameFile
from chipalign.core.task import Task
from chipalign.core.util import timed_segment, autocleaning_pybedtools


class DistancesToRegulatoryFeatures(Task):
    """
    For each bin in bins_task computes the distance to any of the regulatory features obtained by
    :class:`~chipalign.biomart.regulatory_features.RegulatoryFeature`

    :param genome_version: version of genome to use
    :param cell_type: cell type to use, see :class:`~chipalign.biomart.regulatory_features.RegulatoryFeature`
    :param bins_task: the bins task to process
    """
    genome_version = RegulatoryFeatures.genome_version
    cell_type = RegulatoryFeatures.cell_type

    bins_task = luigi.Parameter()

    @property
    def _extension(self):
        return 'pd'

    @property
    def _output_class(self):
        return DataFrameFile

    @property
    def regulatory_features_task(self):
        return RegulatoryFeatures(genome_version=self.genome_version,
                                  cell_type=self.cell_type)

    def requires(self):
        return self.regulatory_features_task, self.bins_task

    def run(self):
        logger = self.logger()

        bins_task_abspath = os.path.abspath(self.bins_task.output().path)
        regulatory_features_abspath = os.path.abspath(self.regulatory_features_task.output().path)

        with autocleaning_pybedtools() as pybedtools:
            bins = pybedtools.BedTool(bins_task_abspath)

            logger.debug('bins_task_abspath: {}'.format(bins_task_abspath))
            features_bedtool = pybedtools.BedTool(regulatory_features_abspath)

            feature_types = {row.name for row in features_bedtool}

            feature_columns = []
            for feature in feature_types:
                with timed_segment('Processing feature: {!r}'.format(feature)):
                    features_filtered = features_bedtool.filter(lambda x: x.name == feature)
                    closest_to_feature = bins.closest(features_filtered, D='ref', t='first')

                    closest_df = closest_to_feature.to_dataframe()
                    closest_df = closest_df[['chrom', 'start', 'end', 'thickEnd']]
                    closest_df.columns = ['chromosome', 'start', 'end', feature]

                    closest_df = closest_df.set_index(['chromosome', 'start', 'end'])[feature]
                    if closest_df.dtype != int:
                        logger.debug('closest_df.dtype: {} is not int, '
                                     'probably there are loci that are not matched'.format(closest_df.dtype))
                        closest_df = closest_df[closest_df != '.'].astype(int)

                    feature_columns.append(closest_df)

            feature_df = pd.concat(feature_columns, axis=1)

            logger.info('Sorting indices and outputting')

            feature_df.sort_index(axis=0, inplace=True)
            feature_df.sort_index(axis=1, inplace=True)

            self.output().dump(feature_df)
