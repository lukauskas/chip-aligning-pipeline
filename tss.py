from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from io import StringIO
import logging
import os
import pybedtools
import requests
import pandas as pd
import tempfile
import shutil

from task import Task, luigi, GzipOutputFile
from profile.genome_wide import GenomeWideProfileBase


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

    @property
    def _extension(self):
        return 'csv.gz'

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

        logger.debug('Writing to csv')
        # Were done for csv export
        with self.output().open('w') as f:
            parsed_data.to_csv(f, index=False)

class BedTranscriptionStartSites(Task):

    genome_version = TranscriptionStartSites.genome_version

    extend_5_to_3 = luigi.IntParameter(default=0)
    extend_3_to_5 = luigi.IntParameter(default=0)

    def requires(self):
        return TranscriptionStartSites(genome_version=self.genome_version)

    @property
    def parameters(self):
        params = self.requires().parameters
        params.append('d{}'.format(self.extend_5_to_3))
        params.append('u{}'.format(self.extend_3_to_5))
        return params

    @property
    def _extension(self):
        return 'bed.gz'

    def run(self):
        logger = self.logger()

        logger.debug('Reading CSV')
        with self.input().open('r') as f:
            tss_data = pd.read_csv(f)

        def _to_bed_format(df):
            for ix, row in df.iterrows():
                bed_row = []
                bed_row.append(row['chromosome_name'])
                bed_row.append(row['transcription_start_site'])
                bed_row.append(row['transcription_start_site'] + 1)
                bed_row.append(row['external_gene_name'])
                bed_row.append('0')   # score
                bed_row.append(row['strand'])

                yield tuple(bed_row)

        try:
            logger.debug('Parsing TSS csv')
            tss_bed = pybedtools.BedTool(list(_to_bed_format(tss_data)))
            logger.debug('Number of TSSs: {}'.format(len(tss_bed)))
            logger.debug('First item: {}'.format(tss_bed[0]))
            logger.debug('Getting chromsizes')
            tss_bed = tss_bed.set_chromsizes(pybedtools.chromsizes(self.genome_version))
            logger.debug('bedtools.slop')
            tss_bed = tss_bed.slop(l=self.extend_3_to_5, r=self.extend_5_to_3, s=True)
            logger.debug('First item after slop: {}'.format(tss_bed[0]))
            logger.debug('Writing to output')
            with self.output().open('w') as out_:
                for line in tss_bed:
                    out_.write(str(line))

        finally:
            logger.debug('Cleaning up')
            pybedtools.cleanup()

class TssGenomeWideProfile(GenomeWideProfileBase):
    binary = True

    extend_5_to_3 = BedTranscriptionStartSites.extend_5_to_3
    extend_3_to_5 = BedTranscriptionStartSites.extend_3_to_5

    @property
    def features_to_map_task(self):
        return BedTranscriptionStartSites(genome_version=self.genome_version,
                                          extend_5_to_3=self.extend_5_to_3,
                                          extend_3_to_5=self.extend_3_to_5)

    @property
    def friendly_name(self):
        return 'near_tss'

if __name__ == '__main__':
    TranscriptionStartSites.logger().setLevel(logging.DEBUG)
    BedTranscriptionStartSites.logger().setLevel(logging.DEBUG)
    logging.basicConfig()
    luigi.run()
