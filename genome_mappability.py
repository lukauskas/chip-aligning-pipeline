from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import luigi
import tarfile
import shutil
from task import Task
from downloader import fetch
import numpy as np

class GenomeMappabilityTrack(Task):

    genome_version = luigi.Parameter()
    read_length = luigi.IntParameter()

    @property
    def _track_uri(self):
        if self.genome_version == 'hg19':
            if 20 <= self.read_length <= 54:
                return 'http://egg2.wustl.edu/roadmap/data/byFileType/mappability/encodeHg19Male/globalmap_k20tok54.tgz'
            else:
                raise Exception('Read length: {} unsupported for {}'.format(self.read_length, self.genome_version))
        else:
            raise Exception('Mappability track unsupported for {}'.format(self.genome_version))

    @property
    def parameters(self):
        return [self.genome_version, self.read_length]

    @property
    def _extension(self):
        return 'npz'

    def run(self):
        logger = self.logger()

        self.ensure_output_directory_exists()
        output_abspath = os.path.abspath(self.output().path)

        with self.temporary_directory(dir='/home/saulius'):
            uri = self._track_uri

            logger.debug('Fetching the mappability data from {}'.format(uri))
            track_file = os.path.basename(uri)
            with open(track_file, 'w') as buffer:
                fetch(uri, buffer)

            logger.debug('Processing {}'.format(track_file))

            processed_tracks = {}

            with tarfile.open(track_file) as tar:
                for member in tar.getmembers():
                    if not member.isfile():
                        continue

                    name = os.path.basename(member.name)
                    chromosome, dtype_str, __ = name.split('.')
                    logger.debug('Chromosome {}'.format(chromosome))
                    assert chromosome not in processed_tracks # Sanity checks

                    if dtype_str == 'uint8':
                        dtype = np.uint8
                    else:
                        raise Exception('Unknown dtype: {}'.format(dtype_str))

                    file_ = tar.extractfile(member)
                    mappability_track = np.frombuffer(file_.read(), dtype=dtype)

                    # straight from http://egg2.wustl.edu/roadmap/data/byFileType/mappability/README
                    mappability_track = (mappability_track > 0) & (mappability_track <= self.read_length)

                    processed_tracks[chromosome] = mappability_track

            logger.debug('Dumping the data to npz archive')
            tmp_location_for_archive = 'mappability.npz'
            np.savez_compressed(tmp_location_for_archive, **processed_tracks)

            logger.debug('Verifying data was written correctly')
            processed_tracks_loaded = np.load(tmp_location_for_archive)

            if sorted(processed_tracks_loaded.keys()) != sorted(processed_tracks.keys()):
                raise IOError('Problem dumping tracks to archive. Keys dont match')

            for key in processed_tracks.keys():
                if not np.equal(processed_tracks[key], processed_tracks_loaded[key]).all():
                    raise IOError('Problem dumping tracks to archive. Data for {} does not match'.format(key))

            logger.debug('Moving file to correct location')
            shutil.move(tmp_location_for_archive, output_abspath)


if __name__ == '__main__':
    import logging
    GenomeMappabilityTrack.logger().setLevel(logging.DEBUG)
    logging.basicConfig()
    luigi.run(main_task_cls=GenomeMappabilityTrack)




