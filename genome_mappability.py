from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from itertools import imap
import os
import luigi
import tarfile
import shutil
import tempfile
import pybedtools
from genome_windows import NonOverlappingWindows
from task import Task
from downloader import fetch
import numpy as np
import logging


class MappabilityTrack(object):

    __lookup_dict = None

    def __init__(self, lookup_dict):
        self.__lookup_dict = lookup_dict

    def filter_uniquely_mappables(self, bedtool):
        logger = logging.getLogger(self.__class__.__name__)
        chromosomes = set(imap(lambda x: x.chrom, bedtool))

        answer = []
        for chromosome in chromosomes:
            logger.debug('Processing {}'.format(chromosome))
            chromosome_lookup = self.__lookup_dict[chromosome]

            reads_for_chromosome = filter(lambda x: x.chrom == chromosome, bedtool)

            logger.debug('Processing {} reads'.format(len(reads_for_chromosome)))

            # Now the start coordinate is the one we need to check, irregardless of strand
            # this is how one should interpret the README
            # http://egg2.wustl.edu/roadmap/data/byFileType/mappability/README
            is_unique = lambda x: chromosome_lookup[x.start]

            chromosome_answer = filter(is_unique, reads_for_chromosome)

            logger.debug('Done. Extending answer')
            answer.extend(chromosome_answer)
            logger.debug('Done')

        return pybedtools.BedTool(answer)

    @classmethod
    def maximum_mappability_score_for_bin(cls, bin_width, read_length, extension_length):
        # see number_of_uniquely_mappable_within_a_bin below
        # [bin_.start + bin_.width] - (bin_.start - (extension_length + read_length - 1))
        # +
        # [bin_.start + bin_.width] + extension_length - (bin_.start - (read_length - 1))
        # ------------------------------------------------------------------------------
        # bin_.width + extension_length + read_length - 1
        # +
        # bin_.width + extension_length + read_length - 1
        return 2 * (bin_width + extension_length + read_length - 1)

    def number_of_uniquely_mappable_within_a_bin(self, bins_bed, read_length, extension_length):
        logger = logging.getLogger(self.__class__.__name__)

        chromosomes = set(imap(lambda x: x.chrom, bins_bed))

        answer = []
        for chromosome in chromosomes:
            logger.debug('Processing {}'.format(chromosome))
            chromosome_lookup = self.__lookup_dict[chromosome]
            chromosome_length = len(chromosome_lookup)

            bins_for_chromosome = filter(lambda x: x.chrom == chromosome, bins_bed)

            for bin_ in bins_for_chromosome:

                # Positive strand
                min_anchor_location = bin_.start - (extension_length + read_length - 1)  # (inclusive)
                max_anchor_location = bin_.end  # (not inclusive)

                min_anchor_location = max(0, min_anchor_location)
                max_anchor_location = min(chromosome_length, max_anchor_location)

                uniquely_mappable_per_bin = np.sum(chromosome_lookup[min_anchor_location:max_anchor_location])

                # Negative strand
                min_anchor_location = bin_.start - (read_length - 1)  # (inclusive)
                max_anchor_location = bin_.end + extension_length  # (not inclusive)

                min_anchor_location = max(0, min_anchor_location)
                max_anchor_location = min(chromosome_length, max_anchor_location)

                uniquely_mappable_per_bin += np.sum(chromosome_lookup[min_anchor_location:max_anchor_location])

                answer.append((bin_.chrom, bin_.start, bin_.end, uniquely_mappable_per_bin))

        return pybedtools.BedTool(answer)


    def is_uniquely_mappable(self, chromosome, start, end, strand):
        chromosome_lookup = self.__lookup_dict[chromosome]

        # Now the start coordinate is the one we need to check, irregardless of strand
        # this is how one should interpret the README
        # http://egg2.wustl.edu/roadmap/data/byFileType/mappability/README
        anchor_locus = int(start)

        return chromosome_lookup[anchor_locus]

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

    def load(self):
        data = np.load(self.path)
        return MappabilityTrack(data)


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

class MappabilityOfGenomicWindows(Task):
    genome_version = NonOverlappingWindows.genome_version
    chromosomes = NonOverlappingWindows.chromosomes
    window_size = NonOverlappingWindows.window_size

    read_length = GenomeMappabilityTrack.read_length

    ext_size = luigi.IntParameter()

    @property
    def non_overlapping_windows_task(self):
        return NonOverlappingWindows(genome_version=self.genome_version,
                                     window_size=self.window_size,
                                     chromosomes=self.chromosomes)

    @property
    def mappability_track_task(self):
        return GenomeMappabilityTrack(genome_version=self.genome_version,
                                      read_length=self.read_length)

    def requires(self):
        return [self.non_overlapping_windows_task, self.mappability_track_task]

    @property
    def parameters(self):
        non_overlapping_windows_task_params = self.non_overlapping_windows_task.parameters
        mappability_track_task_params = self.mappability_track_task.parameters

        specific_parameters = ['ext{}'.format(self.ext_size)]

        return non_overlapping_windows_task_params + mappability_track_task_params + specific_parameters

    @property
    def _extension(self):
        return 'bed.gz'

    def best_total_score(self):
        return MappabilityTrack.maximum_mappability_score_for_bin(bin_width=self.window_size,
                                                                  read_length=self.read_length,
                                                                  extension_length=self.ext_size)

    def run(self):
        logger = self.logger()
        genomic_windows = pybedtools.BedTool(self.non_overlapping_windows_task.output().path)
        mappability = self.mappability_track_task.output().load()

        logger.debug('Computing mappability')
        number_of_uniquely_mappable_per_bin = mappability.number_of_uniquely_mappable_within_a_bin(genomic_windows,
                                                                                                   read_length=self.read_length,
                                                                                                   extension_length=self.ext_size)

        logger.debug('Writing output')
        with self.output().open('w') as output:
            for row in number_of_uniquely_mappable_per_bin:
                output.write(str(row))

class FullyMappableGenomicWindows(Task):

    genome_version = MappabilityOfGenomicWindows.genome_version
    chromosomes = MappabilityOfGenomicWindows.chromosomes
    window_size = MappabilityOfGenomicWindows.window_size

    read_length = MappabilityOfGenomicWindows.read_length

    ext_size = MappabilityOfGenomicWindows.ext_size

    @property
    def genome_windows_mappability_task(self):
        return MappabilityOfGenomicWindows(genome_version=self.genome_version,
                                           chromosomes=self.chromosomes,
                                           window_size=self.window_size,
                                           read_length=self.read_length,
                                           ext_size=self.ext_size)

    def requires(self):
        return self.genome_windows_mappability_task

    @property
    def _extension(self):
        return 'bed.gz'

    @property
    def parameters(self):
        return self.genome_windows_mappability_task.parameters


    def run(self):

        logger = self.logger()

        max_score = self.genome_windows_mappability_task.best_total_score()

        logger.debug('Maximum possible score is: {}'.format(max_score))

        counter_total = 0
        counter_fully_mappable = 0

        with self.output().open('w') as output_:
            with self.input().open('r') as input_:
                for row in input_:
                    counter_total += 1
                    interval, __, score = row.rpartition('\t')
                    score = int(score)
                    if score == max_score:
                        counter_fully_mappable += 1
                        output_.write(interval + '\n')

        logger.debug('Bins total: {}, bins fully mappable: {} ({}%)'.format(counter_total, counter_fully_mappable,
                                                                            counter_fully_mappable * 100.0 / counter_total))



if __name__ == '__main__':
    GenomeMappabilityTrack.logger().setLevel(logging.DEBUG)
    MappabilityOfGenomicWindows.logger().setLevel(logging.DEBUG)
    FullyMappableGenomicWindows.logger().setLevel(logging.DEBUG)
    logging.getLogger(MappabilityTrack.__class__.__name__).setLevel(logging.DEBUG)

    logging.basicConfig()
    luigi.run()




