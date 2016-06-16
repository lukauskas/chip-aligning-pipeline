from chipalign.core.downloader import fetch
from chipalign.core.task import Task
import luigi

from chipalign.core.util import temporary_file
import pandas as pd

from chipalign.database.encode.cell_lines import encode_to_roadmap


def _find_roadmap(encode_cell_line):
    try:
        return encode_to_roadmap(encode_cell_line)
    except KeyError:
        return None

class EncodeTFMetadata(Task):

    genome_version = luigi.Parameter(default='hg19')

    def url(self):
        return 'https://www.encodeproject.org/metadata/type=Experiment&assay_title=ChIP-seq&status=released&assembly={genome}&files.analysis_step_version.analysis_step.pipelines.title=Transcription+factor+ChIP-seq&replication_type=isogenic/metadata.tsv'.format(genome=self.genome_version)

    @property
    def _extension(self):
        return 'csv.gz'

    def run(self):
        logger = self.logger()

        with temporary_file() as temp_filename:

            logger.info('Fetching data table')
            with open(temp_filename, 'w') as fw:
                fetch(self.url(), fw)

            data = pd.read_table(temp_filename)
            data['roadmap_cell_type'] = data['Biosample term name'].apply(_find_roadmap)
            data['target'] = data['Experiment target'].str.replace('-human$', '')
            data['n_replicates'] = data['Biological replicate(s)'].apply(
                lambda x: len(x.split(', ')))

            logger.info('Outputting')
            with self.output().open('w') as output:
                data.to_csv(output, index=False)


