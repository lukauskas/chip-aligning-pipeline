from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import luigi
import tarfile
import shutil
import tempfile
from task import Task
from downloader import fetch
import numpy as np
import logging
from util import temporary_directory


class MappabilityTrack(object):

    __lookup_dict = None

    def __init__(self, lookup_dict):
        self.__lookup_dict = lookup_dict

    def is_uniquely_mappable(self, locus, strand):
        pass


class MappabilityInfoFile(luigi.File):

    def dump(self, data, verify=True):
        logger = logging.getLogger('MappabilityInfoFile.dump')

        __, tmp_location_for_archive = tempfile.mkstemp(suffix='.npz')

        try:
            logger.debug('Dumping the data to npz archive')
            np.savez_compressed(tmp_location_for_archive, **data)

            if verify:
                logger.debug('Verifying data was written correctly')
                processed_tracks_loaded = np.load(tmp_location_for_archive)

                if sorted(processed_tracks_loaded.keys()) != sorted(data.keys()):
                    raise IOError('Problem dumping tracks to archive. Keys don\'t match')

                for key in data.keys():
                    if not np.equal(data[key], processed_tracks_loaded[key]).all():
                        raise IOError('Problem dumping tracks to archive. Data for {} does not match'.format(key))

            logger.debug('Moving file to correct location')
            shutil.move(tmp_location_for_archive, self.path)
        finally:
            try:
                os.unlink(tmp_location_for_archive)
            except OSError:
                if os.path.isfile(tmp_location_for_archive):
                    raise


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
        return [self.genome_version, 'k{}'.format(self.read_length)]

    @property
    def _extension(self):
        return 'npz'

    def output(self):
        super_output = super(GenomeMappabilityTrack, self).output()
        return MappabilityInfoFile(super_output.path)

    def run(self):
        logger = self.logger()

        self.ensure_output_directory_exists()

        processed_tracks = {}

        with self.temporary_directory():
            uri = self._track_uri

            logger.debug('Fetching the mappability data from {}'.format(uri))
            track_file = os.path.basename(uri)
            with open(track_file, 'w') as buffer:
                fetch(uri, buffer)

            logger.debug('Processing {}'.format(track_file))

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

        logger.debug('Saving output')
        self.output().dump(processed_tracks)


if __name__ == '__main__':
    GenomeMappabilityTrack.logger().setLevel(logging.DEBUG)
    logging.basicConfig()
    luigi.run(main_task_cls=GenomeMappabilityTrack)




