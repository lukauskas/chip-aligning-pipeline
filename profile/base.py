from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import luigi

import pandas as pd

from task import Task


def extend_intervals_to_length_in_5to3_direction(intervals, target_length, chromsizes):
    import pybedtools
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

def weighted_means_from_intersection(intersection, column, null_value, mean_function=None):

    def _interval_a_grouped_iterator(intersection):
        previous_interval_a = None
        group_ = None

        for row in intersection:
            interval_a = row[:3]
            rest = row[3:]

            if interval_a != previous_interval_a:
                if previous_interval_a is not None:
                    yield previous_interval_a, group_
                previous_interval_a = interval_a
                group_ = []

            group_.append(rest)

        if previous_interval_a is not None:
            yield previous_interval_a, group_

    def _arithmetic_mean(data):
        weighed_sum = 0
        total_weight = 0
        for value, weight in data:
            weighed_sum += value * weight
            total_weight += weight

        return weighed_sum / total_weight

    if mean_function is None:
        mean_function = _arithmetic_mean

    for interval_a, interval_bs in _interval_a_grouped_iterator(intersection):
        chr_a, start_a, end_a = interval_a
        start_a = int(start_a)
        end_a = int(end_a)

        a_length = end_a - start_a

        sum_parts = []

        last_b = None
        for interval_b in interval_bs:
            start_b = int(interval_b[1])
            if start_b == -1:
                continue

            if last_b is not None and start_b < last_b:
                raise Exception('overlapping intervals in b')

            end_b = int(interval_b[2])
            last_b = end_b

            bases_explained_by_b = min(end_a, end_b) - max(start_a, start_b)
            score = float(interval_b[column - 1])
            sum_parts.append((score, bases_explained_by_b))

        unexplained_bases = a_length - sum([w for __, w in sum_parts])
        sum_parts.append((null_value, unexplained_bases))

        yield (chr_a, start_a, end_a, mean_function(sum_parts))

def weighted_means(sorted_a, sorted_b, column, null_value, mean_function=None):
    intersection = sorted_a.intersect(sorted_b, loj=True, sorted=True)
    return weighted_means_from_intersection(intersection, column, null_value, mean_function=mean_function)

def compute_profile(windows_task_output_abspath, peaks_task_output_abspath,
                    binarise,
                    genome_version=None,
                    logger=None,
                    operation='count', column=None, null_value=None,
                    extend_to_length=None,
                    weighted_mean_function=None):

    def _debug(*args, **kwargs):
        if logger:
            logger.debug(*args, **kwargs)
    def _warn(*args, **kwargs):
        if logger:
            logger.warn(*args, **kwargs)

    import pybedtools
    try:
        windows = pybedtools.BedTool(windows_task_output_abspath)
        peaks = pybedtools.BedTool(peaks_task_output_abspath)

        __, peaks_ext = os.path.splitext(peaks_task_output_abspath)
        if peaks_ext == '.bam':
            _debug('Peaks are in BAM format, converting to bed')
            # This is needed as peaks.sort() doesn't work for BAMs
            peaks = peaks.bam_to_bed()

        if extend_to_length is not None:
            if genome_version is None:
                raise ValueError('Genome version needs to be set for extend_to_length to work')
            _debug('Extending intervals to {}'.format(extend_to_length))
            peaks = extend_intervals_to_length_in_5to3_direction(peaks,
                                                                 extend_to_length,
                                                                 pybedtools.chromsizes(genome_version))
        _debug('Number of windows: {}'.format(len(windows)))
        _debug('Number of peaks: {}'.format(len(peaks)))

        def _to_df_dict(bed_row, value=None):
                d = {'chromosome': bed_row.chrom,
                      'start': bed_row.start,
                      'end': bed_row.end,
                     }

                if value is None:
                    value = float(bed_row.fields[-1])   # .count forces an int

                    if binarise:
                        value = 1 if value > 0 else 0
                    has_value_column = 1
                else:
                    has_value_column = 0

                d['value'] = value

                if len(bed_row.fields) > 3 + has_value_column:
                    d['name'] = bed_row.name

                if len(bed_row.fields) > 4 + has_value_column:
                    d['score'] = bed_row.score

                if len(bed_row.fields) > 5 + has_value_column:
                    d['strand'] = bed_row.strand

                return d

        if len(peaks) == 0:
            _warn('No peaks found!')
            _debug('Windows heads: {}'.format(windows.head()))
            df = pd.DataFrame(map(lambda x: _to_df_dict(x, value=0), windows))
        else:
            _debug('Sorting peaks')
            peaks = peaks.sort()

            _debug('Computing the intersection')

            if operation == 'count':
                null_value = 0 if null_value is None else null_value
                column = 5 if column is None else column
                map_function = lambda x: windows.map(x, o=operation, null=null_value, c=column)
            elif operation in ['sum', 'min', 'max', 'absmin', 'absmax', 'mean', 'median', 'antimode']:
                column = 5 if column is None else column
                null_value = '.' if null_value is None else null_value
                map_function = lambda x: windows.map(x, o=operation, null=null_value, c=column)
            elif operation == 'weighted_mean':
                column = 4 if column is None else column
                null_value = 0 if null_value is None else null_value
                map_function = lambda x: pybedtools.BedTool(list(weighted_means(windows, x,
                                                                           column=column,
                                                                           null_value=null_value,
                                                                           mean_function=weighted_mean_function)))
            else:
                raise ValueError('Unsupported Operation')

            map_ = map_function(peaks)
            _debug('Length of map: {}'.format(len(map_)))

            _debug('Creating dataframe')
            df = pd.DataFrame(map(_to_df_dict, map_))
        # Force column order
        #df = df[['chromosome', 'start', 'end', 'name', 'score', 'strand', 'value']]
        return df

    finally:
        pybedtools.cleanup()

class ProfileBase(Task):

    binary = luigi.BooleanParameter()
    genome_version = luigi.Parameter()
    extend_to_length = luigi.Parameter(default=None)

    @property
    def features_to_map_task(self):
        raise NotImplementedError

    @property
    def areas_to_map_to_task(self):
        raise NotImplementedError

    def requires(self):
        return [self.areas_to_map_to_task, self.features_to_map_task]

    @property
    def parameters(self):
        return [self.genome_version, self.binary, self.extend_to_length]

    @property
    def _extension(self):
        return 'csv.gz'

    def _compute_profile_kwargs(self):
        return dict(operation='count', null_value=0)


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
            output.to_csv(o, index=False)

    def run(self):
        output = self._create_output()
        self.logger().debug(output.head())
        self._save_output(output)
