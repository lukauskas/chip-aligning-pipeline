from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os

import pybedtools
import pandas as pd

from task import Task


def extend_intervals_to_length_in_5to3_direction(intervals, target_length, chromsizes):
    target_length = int(target_length)

    new_intervals = []
    for interval in intervals:
        interval_length = interval.length
        extend_by = target_length - interval_length
        if extend_by < 0:
            raise Exception('Interval {} is already longer than target length {}'.format(interval, target_length))

        if interval.strand == '+':
            interval.end = min(interval.end + extend_by, chromsizes[interval.chrom][1])
        elif interval.strand == '-':
            interval.start = max(interval.start - extend_by, chromsizes[interval.chrom][0])
        else:
            raise Exception('Unknown strand for interval {}'.format(interval))

        new_intervals.append(interval)

    return pybedtools.BedTool(new_intervals)

def compute_profile(windows_task_output_abspath, peaks_task_output_abspath,
                    binarise,
                    genome_version,
                    logger=None,
                    operation='count', column=None, null_value=None,
                    extend_to_length=None):

    def _debug(*args, **kwargs):
        if logger:
            logger.debug(*args, **kwargs)

    try:
        windows = pybedtools.BedTool(windows_task_output_abspath)
        peaks = pybedtools.BedTool(peaks_task_output_abspath)

        __, peaks_ext = os.path.splitext(peaks_task_output_abspath)
        if peaks_ext == '.bam':
            _debug('Peaks are in BAM format, converting to bed')
            # This is needed as peaks.sort() doesn't work for BAMs
            peaks = peaks.bam_to_bed()

        if extend_to_length is not None:
            _debug('Extending intervals to {}'.format(extend_to_length))
            peaks = extend_intervals_to_length_in_5to3_direction(peaks,
                                                                 extend_to_length,
                                                                 pybedtools.chromsizes(genome_version))
        _debug('Sorting peaks')
        peaks = peaks.sort()

        _debug('Computing the intersection')

        if operation == 'count':
            null_value = 0 if null_value is None else null_value
            column = 5
        elif operation in ['sum', 'min', 'max', 'absmin', 'absmax', 'mean', 'median', 'antimode']:
            column = 5 if column is None else column
            null_value = '.' if null_value is None else null_value
        else:
            raise ValueError('Unsupported Operation')

        map_ = windows.map(peaks, o=operation, null=null_value, c=column)

        def _to_df_dict(bed_row):
            d = {'chromosome': bed_row.chrom,
                  'start': bed_row.start,
                  'end': bed_row.end,
                 }

            value = float(bed_row.fields[-1])   # .count forces an int

            if binarise:
                value = 1 if value > 0 else 0

            d['value'] = value

            if len(bed_row.fields) > 4:
                d['name'] = bed_row.name

            if len(bed_row.fields) > 5:
                d['score'] = bed_row.score

            if len(bed_row.fields) > 6:
                d['strand'] = bed_row.strand

            return d
        _debug('Creating dataframe')
        df = pd.DataFrame(map(_to_df_dict, map_))
        # Force column order
        df = df[['chromosome', 'start', 'end', 'name', 'score', 'strand', 'value']]
        return df

    finally:
        pybedtools.cleanup()

class ProfileBase(Task):

    @property
    def features_to_map_task(self):
        raise NotImplementedError

    @property
    def areas_to_map_to_task(self):
        raise NotImplementedError

    def requires(self):
        return [self.areas_to_map_to_task, self.features_to_map_task]

    @property
    def _extension(self):
        return 'csv.gz'

    def _create_output(self):
        logger = self.logger()

        areas_to_map_task_output = self.areas_to_map_to_task.output()

        if isinstance(self.features_to_map_task.output(), tuple) and len(self.features_to_map_task.output()) == 2:
            features_to_map_task_output = self.features_to_map_task.output()[0]
        else:
            features_to_map_task_output = self.features_to_map_task.output()
        if isinstance(features_to_map_task_output, list):
            assert len(features_to_map_task_output) == 2
            features_to_map_task_output = features_to_map_task_output[0]

        map_df = compute_profile(os.path.abspath(areas_to_map_task_output.path),
                        os.path.abspath(features_to_map_task_output.path),
                        self.binary,
                        self.genome_version,
                        logger=logger,
                        extend_to_length=self.extend_to_length,
                        **self._compute_profile_kwargs()
                        )

        return map_df

    def _save_output(self, output):
        logger = self.logger()
        logger.debug('Writing output')

        with self.output().open('w') as o:
            output.to_csv(o)

    def run(self):
        output = self._create_output()
        self._save_output(output)
