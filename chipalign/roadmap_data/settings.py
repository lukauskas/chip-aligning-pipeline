import os
import pandas as pd

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