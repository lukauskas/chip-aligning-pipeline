from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from collections import OrderedDict
from io import StringIO
import logging
import requests
import pandas as pd
import numpy as np

logger = logging.getLogger('roadmap_epigenomics_fetcher')

SEARCH_URI = 'http://www.ncbi.nlm.nih.gov/geo/roadmap/epigenomics/?view=samples&sort=experiment&search={query}&mode=csv'
ENA_SEARCH_URI = 'https://www.ebi.ac.uk/ena/data/warehouse/filereport?accession={}&result=read_run&fields=study_accession,sample_accession,experiment_accession,experiment_alias'

def fetch_csv_results(query):

    uri = SEARCH_URI.format(query=query)
    logger.debug('Fetching: {}'.format(uri))
    response = requests.get(uri)
    response.raise_for_status()

    response_io = StringIO(response.text)
    response_csv = pd.read_csv(response_io)
    response_csv = response_csv[response_csv['Sample Name'] == query]

    data = []

    for ix, row in response_csv.iterrows():
        experiment = row['Experiment']
        geo_accession = row['# GEO Accession']
        sra_ftp = row['SRA FTP']

        try:
            srx_id = sra_ftp.split('/')[-1]
        except AttributeError:
            if np.isnan(sra_ftp):
                logger.warn('Skipping {} ({}) as no SRA FTP specified'.format(experiment, geo_accession))
                continue
            else:
                raise
        assert srx_id.startswith('SRX')


        ena_uri = ENA_SEARCH_URI.format(srx_id)
        logger.debug('Fetching {}'.format(ena_uri))

        ena_response = requests.get(ena_uri)
        ena_response.raise_for_status()
        ena_csv = pd.read_csv(StringIO(ena_response.text), sep='\t')

        assert len(ena_csv) > 0

        ena_csv = ena_csv.iloc[0]
        study_accession = ena_csv['study_accession']
        sample_accession = ena_csv['sample_accession']
        experiment_accession = ena_csv['experiment_accession']
        experiment_alias = ena_csv['experiment_alias']


        data.append(dict(study_accession=study_accession,
                         sample_accession=sample_accession,
                         experiment_accession=experiment_accession,
                         experiment_alias=experiment_alias,
                         data_track=experiment,
                         cell_type='H1'))

    data = sorted(data, key=lambda x: (x['sample_accession'], x['data_track']))
    prev_sample_accession = None
    for x in data:
        sample_accession = x.pop('sample_accession')
        if prev_sample_accession is None or sample_accession != prev_sample_accession:
            prev_sample_accession = sample_accession
            print('# -- Sample {} -----'.format(sample_accession))

        experiment_alias = x.pop('experiment_alias')
        print('{!r}, # {}'.format(x, experiment_alias))


if __name__ == '__main__':
    logger.setLevel(logging.DEBUG)
    logging.basicConfig()

    fetch_csv_results('H1 cell line')