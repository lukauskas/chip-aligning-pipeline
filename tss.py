from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import requests
from task import Task, luigi, GzipOutputFile


class TranscriptionStartSites(Task):

    genome_version = luigi.Parameter()

    __ENSEMBL_FRONTENDS = {#'hg19': 'http://grch37.ensembl.org/biomart/martservice',
                           #'hg18': 'http://may2009.ensembl.org/biomart/martservice',
                           'hg38': 'http://www.ensembl.org/biomart/martservice' }


    __BIOMART_QUERY_TEMPLATE = '''<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE Query>
<Query  virtualSchemaName = "default" formatter = "CSV" header = "0" uniqueRows = "0" count = "" datasetConfigVersion = "0.6" >

	<Dataset name = "hsapiens_gene_ensembl" interface = "default" >
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
                'transcription_start_site',
                'strand',
                'chromosome_name',
                'description',
                'percentage_gc_content',
                'external_gene_name']

    def _biomart_query(self):
        attributes_xml = ''.join(['<Attribute name = "{}" />'.format(a) for a in self._attributes_to_fech()])
        return self.__BIOMART_QUERY_TEMPLATE.format(attributes_xml=attributes_xml)

    def _csv_header_row(self):
        return ','.join(self._attributes_to_fech()) + '\n';

    @property
    def parameters(self):
        return [self.genome_version]

    def output(self):
        return GzipOutputFile(super(TranscriptionStartSites, self).output().path)

    @property
    def _extension(self):
        return 'csv.gz'

    def run(self):

        data = dict(query=self._biomart_query())

        response = requests.post(self.__ENSEMBL_FRONTENDS[self.genome_version], data=data)
        response.raise_for_status()

        if response.text.startswith('Query ERROR'):
            raise Exception(response.text)
        elif '<html>' in response.text:
            raise Exception('Something went wrong when querying BioMart, html returned')

        with self.output().open('w') as f:
            f.write(self._csv_header_row())
            f.write(response.text)


if __name__ == '__main__':
    luigi.run(main_task_cls=TranscriptionStartSites)
