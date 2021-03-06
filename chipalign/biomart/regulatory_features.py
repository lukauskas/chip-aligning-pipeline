import luigi
import pandas as pd

from chipalign.biomart.service import fetch_query_from_ensembl
from chipalign.core.task import Task
from chipalign.core.util import temporary_directory, timed_segment

_XML_QUERY = """
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE Query>
<Query  virtualSchemaName = "default" formatter = "CSV" header = "0" uniqueRows = "1" count = "" datasetConfigVersion = "0.6" completionStamp="1">

    <Dataset name = "hsapiens_regulatory_feature" interface = "default" >
        <Filter name = "cell_type_name_1051" value = "{cell_type}"/>
        <Filter name = "chromosome_name" value = "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,X,Y,MT"/>
        <Attribute name = "chromosome_name" />
        <Attribute name = "chromosome_start" />
        <Attribute name = "chromosome_end" />
        <Attribute name = "feature_type_name" />
    </Dataset>
</Query>"""


def _ensembl_regulatory_regions_to_bed(ensembl_regulatory_regions_file):
    data = pd.read_csv(ensembl_regulatory_regions_file, header=None,
                       names=['chromosome', 'start', 'end', 'feature'])

    # Convert to bed indexing format
    data['chromosome'] = data['chromosome'].apply(lambda x: 'chr{}'.format(x) if x != 'MT' else 'chrM')
    data['start'] -= 1
    data['end'] -= 1

    data['name'] = data['feature']

    bed_data = data[['chromosome', 'start', 'end', 'name']]
    bed_data = bed_data.sort(['chromosome', 'start', 'end'])
    bed_data = bed_data.drop_duplicates()

    return bed_data


class RegulatoryFeatures(Task):
    """
    Downloads the regulatory feature information from Ensembl and stores it in BED format
    :param genome_version: genome version to use (in 'hgxx' format, code will convert it to ENSEMBL)
    :param cell_type: cell type to download features for.
                      will return an aggregation of cells if set to 'aggregated'
    """
    genome_version = luigi.Parameter()
    cell_type = luigi.Parameter(default='aggregated')

    @property
    def _extension(self):
        return 'bed.gz'

    _ENSEMBL_CELL_TYPE_MAP = {
        'E114': 'A549',
        'E115': 'DND-41',
        'E116': 'GM12878',
        'E003': 'H1ESC',
        'E117': 'HeLa-S3',
        'E118': 'HepG2',
        'E119': 'HMEC',
        'E120': 'HSMM',
        'E121': 'HSMMtube',
        'E122': 'HUVEC',
        'E123': 'K562',
        'E124': 'Nonocytes-CD14+',
        'E125': 'NH-A',
        'E126': 'NHDF-AD',
        'E127': 'NHEK',
        'E128': 'NHLF',
        'E129': 'Osteobl',
        'aggregated': 'MultiCell'
    }

    def _ensembl_cell_type(self):
        try:
            return self._ENSEMBL_CELL_TYPE_MAP[self.cell_type]
        except KeyError:
            raise KeyError('Unsupported cell type {!r}'.format(self.cell_type))

    def _ensembl_query(self):
        logger = self.logger()
        query = _XML_QUERY.format(cell_type=self._ensembl_cell_type())
        logger.debug('Generated Ensembl Query query:\n{}'.format(query))
        return query

    def _run(self):
        logger = self.logger()

        with temporary_directory(logger=logger):
            with timed_segment('Downloading Regulatory Features from Ensembl', logger=logger):
                ensembl_filename = 'regulatory_features.ensembl'
                with open(ensembl_filename, 'w') as f:
                    fetch_query_from_ensembl(self.genome_version, self._ensembl_query(), f)

            bed_data = _ensembl_regulatory_regions_to_bed(ensembl_filename)

        logger.info('Writing to {}'.format(self.output().path))
        with self.output().open('w') as bed_file:
            bed_data.to_csv(bed_file, index=False, header=False, sep=str('\t'))
