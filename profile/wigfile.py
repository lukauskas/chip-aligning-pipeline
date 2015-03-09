from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import re
import itertools
import numpy as np
import pandas as pd

from task import GzipOutputFile
from util import memoised


@memoised
def _number_of_windows(genome_assembly, window_size):
    from pybedtools import chromsizes as pybedtools_chromsizes

    chromsizes = pybedtools_chromsizes(genome_assembly)

    number_of_windows = {}
    for chromosome, chromsize in chromsizes.iteritems():

        start, end = chromsize
        length = end - start

        number_of_windows_for_chromosome = length / window_size
        if length % window_size:
            number_of_windows_for_chromosome += 1

        number_of_windows[chromosome] = number_of_windows_for_chromosome

    return number_of_windows

class WigFile(GzipOutputFile):
    _window_size = None
    _genome_assembly = None

    def __init__(self, genome_assembly, window_size, path=None):
        self._genome_assembly = genome_assembly
        self._window_size = window_size
        super(WigFile, self).__init__(path=path)

    def to_numpy(self, chromosomes):
        window_size = self._window_size
        number_of_windows = _number_of_windows(self._genome_assembly, window_size)

        with self.open('r') as wigfile_handle:
            data = {}
            current_chromosome = None
            current_array = None

            for line in wigfile_handle:
                if line.startswith('track'):
                    continue
                if line.startswith('variableStep'):
                    match = re.match('variableStep chrom=(chr\w+) span=(\d+)', line)
                    if not match:
                        raise Exception("Cannot parse variableStep line {0!r}".format(line))

                    chromosome, span = match.group(1), match.group(2)
                    span = int(span)

                    assert span == window_size, 'Span does not match the window size'

                    if current_chromosome == chromosome or chromosome in data:
                        raise Exception('Duplicate variableStep line for chromosome')
                    elif current_chromosome is not None:
                        data[current_chromosome] = current_array

                    current_chromosome = chromosome
                    array_size = number_of_windows[chromosome]
                    current_array = np.zeros(array_size, dtype=int)
                else:
                    position, value = line.split('\t')
                    position = int(position)
                    value = float(value)
                    array_index = (position - 1) / window_size
                    assert (position - 1) % window_size == 0, 'expected positions at start of intervals'

                    try:
                        current_array[array_index] = value
                    except IndexError:
                        if array_index >= len(current_array):
                            raise ValueError('Position {0!r} is invalid as it is mapped to {1!r}th cell '
                                             'when only {2!r} cells are available'.format(position,
                                                                                          array_index,
                                                                                          len(current_array)))
                        else:
                            raise

                if current_chromosome is not None:
                    data[current_chromosome] = current_array

            filtered_data = {}

            for chrom, d in data.iteritems():
                if chrom not in chromosomes:
                    continue
                filtered_data[chrom] = d

            for chrom in chromosomes:
                if chrom not in filtered_data:
                    filtered_data[chrom] = np.zeros(number_of_windows[chrom])

            return filtered_data

    def to_pandas_series(self, chromosomes):

        parsed_output = self.to_numpy(chromosomes=chromosomes)

        full_data = []
        index = []

        for chromosome, data in sorted(parsed_output.items(), key=lambda x: x[0]):
            if chromosome not in chromosomes:
                continue

            full_data.extend(data)
            index.extend(zip(itertools.repeat(chromosome), xrange(len(data))))

        index = pd.MultiIndex.from_tuples(index, names=('chromosome', 'window_id'))
        return pd.Series(full_data, index=index)
