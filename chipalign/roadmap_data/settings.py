import os
import pandas as pd

_CONSOLIDATED_METATADATA_FILE = os.path.join(os.path.dirname(__file__), '_qc_metadata/consolidated_epigenomes_qc.csv')

GENOME_ID = 'hg19'
READ_LENGTH = 36


def max_fraglen(cell_line):
    data = pd.read_csv(_CONSOLIDATED_METATADATA_FILE)

    data = data.query('EID == @cell_line and MARK != "DNase"')['FRAGLEN']
    return data.max()
