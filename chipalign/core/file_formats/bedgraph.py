from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import re
import luigi.format

import pandas as pd
from chipalign.core.file_formats.file import File


class BedGraph(File):
    _is_gzipped = None

    def __init__(self, path=None, **kwargs):
        if path.endswith('gz'):
            format_ = kwargs.get('format', luigi.format.Gzip)
            self._is_gzipped = True
        else:
            format_ = kwargs.get('format', None)
            self._is_gzipped = False

        super(BedGraph, self).__init__(path=path, format=format_, **kwargs)

    def first_line_is_header(self):
        with self.open('r') as f:
            line = f.readline().decode('utf-8')

        return line.startswith('track')

    def header(self):
        with self.open('r') as f:
            line = f.readline().decode('utf-8')

        if not line.startswith('track'):
            return {}

        __, __, header = line.partition('track')
        header = header.strip()

        ans = {}
        regexp = re.compile('(?P<key>\w+)=((?P<value>[^" ]*)( |$)|"(?P<value_quoted>[^"]*)")')
        for match in regexp.finditer(header):
            key = match.group('key')

            if match.group('value'):
                value = match.group('value')
            else:
                value = match.group('value_quoted')
            ans[key] = value

        return ans

    def to_pandas_series(self, name=None):

        with self.open('r') as f:
            if self.first_line_is_header():
                skip_rows = 1
            else:
                skip_rows = 0

            series = pd.read_table(f,
                                   header=None, names=['chromosome', 'start', 'end', 'value'],
                                   index_col=['chromosome', 'start', 'end'], skiprows=skip_rows)

        series = series['value']
        if name is None:
            name = self.header().get('name', None)

        series.name = name
        return series


