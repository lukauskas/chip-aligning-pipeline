import logging
import os

import fnmatch
import klepto
import pandas as pd
from functools32 import lru_cache
import requests
import re

_CONSOLIDATED_METATADATA_FILE = os.path.join(os.path.dirname(__file__), '_qc_metadata/consolidated_epigenomes_qc.csv')

GENOME_ID = 'hg19'
READ_LENGTH = 36


def max_fraglen(cell_line):
    data = pd.read_csv(_CONSOLIDATED_METATADATA_FILE)

    data = data.query('EID == @cell_line and MARK != "DNase"')['FRAGLEN']
    return data.max()

COMMON_CELL_TYPE_NAMES = {
    'E008': 'H9',
    'E003': 'H1',
    'E114': 'A549',
    'E115': 'DND-41',
    'E116': 'GM12878',
    'E117': 'HeLa-S3',
    'E118': 'HepG2',
    'E119': 'HMEC',
    'E120': 'HSMM',
    'E121': 'HSMMT',
    'E122': 'HUVEC',
    'E123': 'K562',
    'E124': 'CD14+',
    'E125': 'NH-A',
    'E126': 'NHDF-AD',
    'E127': 'NHEK',
    'E128': 'NHLF',
    'E129': 'Osteoblast',
    'E017': 'IMR90',
}


@lru_cache(None)
def consolidation_summary_metadata():
    data = pd.read_csv(os.path.join(os.path.dirname(__file__),
                                    '_qc_metadata/consolidated_summary.csv'), header=[0, 1])

    # Remove first row as it is just a summary row of counts
    data = data.iloc[1:]

    # Set epigenome id as index
    data = data.set_index('Epigenome ID (EID)')
    # Remove the weird tupling that happens for this column
    data.index = [i[0] for i in data.index]

    return data


def consolidated_filename_patterns(cell_type, target):
    data = consolidation_summary_metadata()
    filenames = data.loc[cell_type, ('Pool Filenames', target)]

    if pd.isnull(filenames):
        return []
    else:
        fnames = filenames.split(';')

        clean_fnames = []
        for fname in fnames:
            # Not entirely sure why they give .bed.gz sometimes
            if fname.endswith('.bed.gz'):
                fname = fname.replace('.bed.gz', '.filt.tagAlign.gz')

            clean_fnames.append(fname)

        return clean_fnames


@lru_cache(None)
def consolidated_read_download_uris(cell_type, target):
    patterns = consolidated_filename_patterns(cell_type, target)
    if not patterns:
        return []

    all_uris = downloadable_unconsolidated_reads()
    uris = []
    for pattern in patterns:
        for uri in all_uris:
            basename = os.path.basename(uri)

            if fnmatch.fnmatchcase(basename, pattern):
                uris.append(uri)

    if not uris:
        logger = logging.getLogger('chipalign.database.roadmap.settings'
                                   '.consolidated_read_download_uris')
        logger.debug('Patterns found for cell_type={!r}, target={!r}:\n{!r}'.format(cell_type,
                                                                                    target,
                                                                                    patterns))

        raise Exception('Patterns found, but no uris could be parsed for cell_type={!r}, '
                        'target={!r}'.format(cell_type, target))
    return uris


@klepto.lru_cache()
def downloadable_unconsolidated_reads():

    root_uri = 'http://egg2.wustl.edu/roadmap/data/byFileType/alignments/unconsolidated/'

    queue = []
    queue.append(root_uri)

    answers = []
    while queue:
        uri = queue.pop()
        response = requests.get(uri)
        response.raise_for_status()

        text = response.text
        hrefs = filter(lambda x: not x.startswith('?') and not x.startswith('/'),
                       re.findall('<a\s+href="([^"]+)"', text))

        for href in hrefs:
            full_uri = ''.join([uri, href])
            if href.endswith('/'):
                queue.append(full_uri)
            elif href.endswith('.tagAlign.gz'):
                answers.append(full_uri)
            else:
                continue

    return answers


@lru_cache(100)
def targets_for_cell_line(cell_line):

    metadata = consolidation_summary_metadata()
    tracks = metadata.loc[cell_line, 'Pool Filenames'].dropna().index

    def is_histone(track):
        return track != 'Input' and track != 'DNase' \
               and track != 'RNA-seq' and track != 'RRBS' \
               and track != 'WGBS' and track != 'mCRF'

    tracks = filter(is_histone, tracks)
    return tracks