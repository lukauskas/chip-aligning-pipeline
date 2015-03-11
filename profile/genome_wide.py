from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import logging
import os
import luigi
from genome_windows import NonOverlappingWindows
from peak_calling.macs import MacsPeaks
from profile.base import compute_profile, ProfileBase
from profile.wigfile import WigFile
from task import Task

def _intersection_counts_to_wiggle(output_file_handle,
                                   intersection_with_counts_bed,
                                   name,
                                   description,
                                   window_size):
    output_file_handle.write('track type=wiggle_0 name="{0}" description="{1}"\n'.format(
        name,
        description
    ))
    previous_chromosome = None
    for row in intersection_with_counts_bed:
        value = row.value
        if value == 0:
            # Do not need to write zeroes to wiggles
            continue

        if row.chromosome != previous_chromosome:
            output_file_handle.write('variableStep chrom={0} span={1}\n'.format(row.chromosome, window_size))
            previous_chromosome = row.chromosome

        # Add +1 to start as wig locations are 1-based
        start = row.start + 1

        output_file_handle.write('{0}\t{1}\n'.format(start, value))

class GenomeWideProfileBase(ProfileBase):
    """
    A base task that generates the profile of the peaks_task output over the
    range of non-overlapping genome windows of size window_size.

    If binary is set to False, the number of overlapping peaks_task outputs will be counted
    whereas if it is set to True, only a binary yes/no response will be returned.
    """

    window_size = NonOverlappingWindows.window_size

    @property
    def parameters(self):
        parameters = self.features_to_map_task.parameters
        parameters.append('w{}'.format(self.window_size))
        if self.binary:
            parameters.append('b')
        if self.extend_to_length is not None:
            parameters.append('e{}'.format(self.extend_to_length))

        return parameters

    @property
    def _extension(self):
        return 'wig.gz'

    @property
    def areas_to_map_to_task(self):
        return NonOverlappingWindows(genome_version=self.genome_version,
                                     window_size=self.window_size)

    @property
    def friendly_name(self):
        raise NotImplementedError


    def output(self):
        super_output_path = super(ProfileBase, self).output().path
        return WigFile(genome_assembly=self.genome_version, window_size=self.window_size, path=super_output_path)

    def _save_output(self, output):
        logger = self.logger()

        logger.debug('Outputting to {}'.format(self.output().path))
        with self.output().open('w') as output_file:
            _intersection_counts_to_wiggle(output_file,
                                           output,
                                           name=self.friendly_name,
                                           description=os.path.basename(output.path),
                                           window_size=self.window_size
                                           )
