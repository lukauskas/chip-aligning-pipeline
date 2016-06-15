from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import tempfile

from chipalign.core.downloader import fetch

_BIOMART_URLS = {'hg19': 'http://grch37.ensembl.org/biomart/martservice?query={}'}


def fetch_query_from_ensembl(genome_version, query, output_file_handle):
    try:
        biomart_service_url = _BIOMART_URLS[genome_version]
    except KeyError:
        raise ValueError('Unsupported genome version {}'.format(genome_version))

    query = query.replace('\n', '')
    url = biomart_service_url.format(query)

    with tempfile.TemporaryFile('w+') as temp_file:
        fetch(url, temp_file)

        # Read temp file now
        temp_file.seek(0)

        last_line = None
        number_of_lines = 0
        for line in temp_file:
            last_line = line
            number_of_lines += 1

        if last_line.strip() != '[success]':
            raise Exception('Could not download the binding motifs from Ensembl. '
                            'The completion stamp not present in data.'
                            'Last line: {!r}'.format(last_line))

        # Write to output
        temp_file.seek(0)

        for i, line in enumerate(temp_file):
            if i < number_of_lines - 1:
                output_file_handle.write(line)
