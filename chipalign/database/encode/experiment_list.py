import requests

from chipalign.core.downloader import fetch
from chipalign.core.task import Task
import luigi

from chipalign.core.util import temporary_file
import pandas as pd
import json
import logging


def _find_signal_p_value_bigwig(experiment_id):
    url = 'https://www.encodeproject.org/experiments/{}/?format=json'.format(experiment_id)
    logger = logging.getLogger('chipalign.database.encode.experiment_list._find_signal_p_value_bigwig')
    logger.debug('Finding data location for {}'.format(url))
    response = requests.get(url)
    response.raise_for_status()

    data = json.loads(response.text)
    n_replicates = len(data['replicates'])

    files = data['files']

    for file_ in files:
        output_type = file_['output_type']
        if output_type != 'signal p-value':
            continue

        n_file_replicates = len(file_['biological_replicates'])
        if n_file_replicates == n_replicates:
            # Found our file
            return file_['accession']

    # Could not find
    return None


class EncodeTFExperimentList(Task):

    genome_version = luigi.Parameter(default='hg19')

    def url(self):
        return 'https://www.encodeproject.org/report.tsv?type=Experiment&assay_term_name=ChIP-seq&replicates.library.biosample.donor.organism.scientific_name=Homo+sapiens&target.investigated_as=transcription+factor&assembly={genome}&status=released&assay_title=ChIP-seq&format=table&limit=all'.format(genome=self.genome_version)

    @property
    def _extension(self):
        return 'csv.gz'

    def run(self):
        logger = self.logger()

        with temporary_file() as temp_filename:

            logger.info('Fetching data table')
            with open(temp_filename, 'w') as fw:
                fetch(self.url(), fw)

            logger.info('Fetching signal accession numbers')
            data = pd.read_table(temp_filename)
            data['signal_accession'] = data['Accession'].apply(_find_signal_p_value_bigwig)

            logger.info('Outputting')
            with self.output().open('w') as output:
                data.to_csv(output, index=False)


