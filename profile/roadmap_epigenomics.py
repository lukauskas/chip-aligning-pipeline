from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from gzip import GzipFile
import os
import logging
import luigi
import shutil
from task import Task
from downloader import fetch
from profile.base import ProfileBase
from profile.genome_wide import GenomeWideProfileBase
from util import temporary_directory, ensure_directory_exists_for_file

import numpy as np

class _RoadmapMixin(object):
    cell_type = luigi.Parameter()
    data_track = luigi.Parameter()
    signal_type = luigi.Parameter(default='pval')
    genome_version = luigi.Parameter()

    def roadmap_parameters(self):
        return [self.cell_type, self.data_track, self.genome_version, self.signal_type]

class RoadmapSignal(_RoadmapMixin, Task):

    _CELL_TYPE_ENCODINGS = {'H9': 'E008'}
    _URI_TEMPLATE = 'http://egg2.wustl.edu/roadmap/data/byFileType/signal/consolidated/macs2signal/' \
                    '{signal_type}/{cell_type_encoding}-{track}.{signal_type}.signal.bigwig'

    @property
    def parameters(self):
        return self.roadmap_parameters()

    def _roadmap_uri(self):
        if self.genome_version != 'hg19':
            raise ValueError('Only hg19 supported')
        else:
            uri = self._URI_TEMPLATE.format(cell_type_encoding=self._CELL_TYPE_ENCODINGS[self.cell_type],
                                            track=self.data_track,
                                            signal_type=self.signal_type)

            return uri

    @property
    def _extension(self):
        return 'bed'

    def run(self):
        from sh import bigWigToBedGraph

        logger = self.logger()
        uri_to_fetch = self._roadmap_uri()

        output_abspath = os.path.abspath(self.output().path)
        ensure_directory_exists_for_file(output_abspath)

        with temporary_directory(prefix='tmp-roadmap-signal', cleanup_on_exception=False, logger=logger):
            logger.debug('Fetching {}'.format(uri_to_fetch))

            tmp_signal_filename = 'signal.bigWig'

            with open(tmp_signal_filename, 'w') as output_:
                fetch(uri_to_fetch, output_)

            tmp_bed_graph_filename = 'signal.bedGraph'

            logger.debug('Converting bigWig to bedGraph')
            bigWigToBedGraph(tmp_signal_filename, tmp_bed_graph_filename)

            # logger.debug('Gzipping')
            # gzip_bed_graph_filename = 'signal.bedGraph.gz'
            # with GzipFile(gzip_bed_graph_filename, 'w') as out_:
            #     with open(tmp_signal_filename, 'r') as in_:
            #         out_.writelines(in_)

            logger.debug('Moving to correct location')
            shutil.move(tmp_bed_graph_filename, output_abspath)

def _log10_weighted_mean(data):
    weighted_sum = 0
    weights = 0

    for value, weight in data:
        value = np.power(10.0, -value)
        weighted_sum += value * weight
        weights += weight

    weighted_sum /= weights
    return - np.log10(weighted_sum)

class RoadmapProfile(GenomeWideProfileBase, _RoadmapMixin):

    @property
    def features_to_map_task(self):
        return RoadmapSignal(genome_version=self.genome_version,
                             cell_type=self.cell_type,
                             data_track=self.data_track,
                             signal_type=self.signal_type,
                             )

    def _compute_profile_kwargs(self):
        if self.signal_type == 'pval':
            mean_function = _log10_weighted_mean
            null_value = 1
        else:
            raise NotImplementedError('Unsupported signal type {!r}'.format(self.signal_type))

        return dict(operation='weighted_mean', column=4, null_value=null_value, weighted_mean_function=mean_function)

if __name__ == '__main__':
    logging.basicConfig()
    RoadmapSignal.logger().setLevel(logging.DEBUG)
    RoadmapProfile.logger().setLevel(logging.DEBUG)

    luigi.run()






