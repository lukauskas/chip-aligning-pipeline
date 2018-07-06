import shutil

from chipalign.core.downloader import fetch
from chipalign.core.task import Task
import luigi

from chipalign.core.util import temporary_file
import pandas as pd
import numpy as np

from chipalign.database.encode.cell_lines import encode_to_roadmap


def _find_roadmap(encode_cell_line):
    try:
        return encode_to_roadmap(encode_cell_line)
    except KeyError:
        return None


class EncodeTFMetadata(Task):

    genome_version = luigi.Parameter(default='hg38')

    def url(self):
        # To obtain this URI go to a search page, i.e.:
        # https://www.encodeproject.org/search/?type=Experiment&assay_title=ChIP-seq&assembly=hg19&status=released&replication_type=isogenic
        # click Download
        # click Download (in the popup)
        # open file that has been downloaded
        # copy the first line
        if self.genome_version.startswith('hg'):
            return 'https://www.encodeproject.org/metadata/type=Experiment&assay_title=ChIP-seq&replicates.library.biosample.donor.organism.scientific_name=Homo+sapiens/metadata.tsv'
        else:
            raise NotImplementedError('Metadata download for genome {} not implemented yet'.format(self.genome_version))

    @property
    def _extension(self):
        return 'csv.gz'

    def _run(self):
        logger = self.logger()

        with temporary_file() as temp_filename:

            logger.info('Fetching data table')
            with open(temp_filename, 'wb') as fw:
                fetch(self.url(), fw)

            data = pd.read_table(temp_filename)
            data['roadmap_cell_type'] = data['Biosample term name'].apply(_find_roadmap)
            data['target'] = data['Experiment target'].str.replace('-human$', '')
            data['is_input'] = data['target'].apply(lambda x: 'control' in x.lower())

            def count_replicates(str_):
                if str_ is None or (isinstance(str_, float) and np.isnan(str_)):
                    return None
                elif isinstance(str_, str):
                    return len(str_.split(', '))
                else:
                    return int(str_)

            data['n_replicates'] = data['Biological replicate(s)'].apply(count_replicates)

            logger.info('Outputting')

            with temporary_file() as tf:
                data.to_csv(tf, index=False, compression='gzip')
                shutil.move(tf, self.output().path)


