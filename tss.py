from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from io import StringIO
import logging
import requests
import pandas as pd

from task import Task, luigi, GzipOutputFile


def _ensembl_to_ucsc_chrom_name(chromosome):
    try:
        return 'chr{}'.format(int(chromosome))
    except ValueError:
        if chromosome in ['X', 'Y']:
            return 'chr{}'.format(chromosome)
        elif chromosome == 'MT':
            return 'chrM'
        else:
            raise ValueError('Unknown ENSEMBL chromosome {}'.format(chromosome))

class TranscriptionStartSites(Task):

    genome_version = luigi.Parameter()
    format = luigi.Parameter('csv')

    __ENSEMBL_FRONTENDS = {#'hg19': 'http://grch37.ensembl.org/biomart/martservice',
                           #'hg18': 'http://may2009.ensembl.org/biomart/martservice',
                           'hg38': 'http://www.ensembl.org/biomart/martservice' }


    __BIOMART_QUERY_TEMPLATE = '''<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE Query>
<Query  virtualSchemaName = "default" formatter = "CSV" header = "0" uniqueRows = "0" count = "" datasetConfigVersion = "0.6" >

	<Dataset name = "hsapiens_gene_ensembl" interface = "default" >
	    <Filter name = "chromosome_name" value = "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,MT,X,Y"/>
		<Filter name = "status" value = "KNOWN"/>
		<Filter name = "transcript_biotype" value = "protein_coding"/>
		<Filter name = "transcript_status" value = "KNOWN"/>
		<Filter name = "with_tra_gencode_basic" excluded = "0"/>
		{attributes_xml}
	</Dataset>
</Query>'''

    def _attributes_to_fech(self):
        return ['ensembl_gene_id',
                'ensembl_transcript_id',
                'chromosome_name',
                'transcription_start_site',
                'strand',
                'external_gene_name',
                'percentage_gc_content',
                'transcript_length',
                'transcript_count',
                'transcript_biotype',
                'transcript_tsl']

    def _biomart_query(self):
        attributes_xml = ''.join(['<Attribute name = "{}" />'.format(a) for a in self._attributes_to_fech()])
        return self.__BIOMART_QUERY_TEMPLATE.format(attributes_xml=attributes_xml)

    @property
    def parameters(self):
        return [self.genome_version]

    def output(self):
        return GzipOutputFile(super(TranscriptionStartSites, self).output().path)

    @property
    def _extension(self):
        if self.format == 'csv':
            return 'csv.gz'
        elif self.format == 'bed':
            return 'bed.gz'
        else:
            raise ValueError('unsupported format {}'.format(self.format))

    def run(self):
        logger = self.logger()

        data = dict(query=self._biomart_query())
        endpoint = self.__ENSEMBL_FRONTENDS[self.genome_version]
        logger.debug('Querying {}'.format(endpoint))
        response = requests.post(endpoint, data=data)
        response.raise_for_status()

        if response.text.startswith('Query ERROR'):
            raise Exception(response.text)
        elif '<html>' in response.text:
            raise Exception('Something went wrong when querying BioMart, html returned')

        logger.debug('Parsing TSS list')
        parsed_data = pd.read_csv(StringIO(response.text), names=self._attributes_to_fech())

        logger.debug('Head of parsed data')
        logger.debug(parsed_data.head())

        logger.debug('Changing chromosome names')
        parsed_data['chromosome_name'] = parsed_data['chromosome_name'].apply(_ensembl_to_ucsc_chrom_name)

        logger.debug('Changing strand names')
        parsed_data['strand'] = parsed_data['strand'].apply(lambda x: '+' if x == 1 else '-')

        logger.debug('Minus one-ing the transcription-start-site')
        parsed_data['transcription_start_site'] -= 1

        logger.debug('Sorting by chromosome name, tss, strand and count')
        parsed_data.sort(['chromosome_name', 'transcription_start_site', 'strand', 'transcript_count'], inplace=True)

        logger.debug('Number of TSS before duplicate removal: {}'.format(len(parsed_data)))
        parsed_data.drop_duplicates(subset=['chromosome_name', 'transcription_start_site', 'strand'],
                                    take_last=True,
                                    inplace=True)
        logger.debug('Number of TSS after duplicate removal: {}'.format(len(parsed_data)))

        if self.format == 'csv':
            logger.debug('Writing to csv')
            # Were done for csv export
            with self.output().open('w') as f:
                parsed_data.to_csv(f, index=False)
            return
        elif self.format == 'bed':
            logger.debug('Writing out to bed')
            with self.output().open('w') as f:
                for ix, row in parsed_data.iterrows():
                    bed_row = []
                    bed_row.append(row['chromosome_name'])
                    bed_row.append(row['transcription_start_site'])
                    bed_row.append(row['transcription_start_site'] + 1)
                    bed_row.append(row['external_gene_name'])
                    bed_row.append('0')   # score
                    bed_row.append(row['strand'])

                    bed_row = '\t'.join(map(str, bed_row)) + '\n'
                    f.write(bed_row)
        else:
            raise ValueError('Don\'t know how to convert to format {}'.format(self.format))


if __name__ == '__main__':
    TranscriptionStartSites.logger().setLevel(logging.DEBUG)
    logging.basicConfig()
    luigi.run(main_task_cls=TranscriptionStartSites)
