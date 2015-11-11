from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from chipalign.core.downloader import fetch
from chipalign.core.task import Task, luigi
from chipalign.core.util import temporary_file

import pandas as pd

class BinsAroundTSS(Task):


    genome_version = luigi.Parameter(default='hg19')
    extend_downstream = luigi.IntParameter(default=500)
    extend_upstream = luigi.IntParameter(default=499)

    @property
    def task_class_friendly_name(self):
        return 'BTSS'

    @property
    def _extension(self):
        return 'bed.gz'


    def _biomart_endpoint(self):
        if self.genome_version == 'hg19':
            return 'http://grch37.ensembl.org/biomart/martservice?query={}'
        else:
            raise ValueError('Unsupported genome version: {}'.format(self.genome_version))

    def _xml_query(self):
        if self.genome_version == 'hg19':
            return """
            <?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE Query>
<Query  virtualSchemaName = "default" formatter = "TSV" header = "0" uniqueRows = "1" count = "" datasetConfigVersion = "0.6" completionStamp="1">

	<Dataset name = "hsapiens_gene_ensembl" interface = "default" >
		<Filter name = "status" value = "KNOWN"/>
		<Filter name = "transcript_status" value = "KNOWN"/>
		<Attribute name = "ensembl_gene_id" />
		<Attribute name = "ensembl_transcript_id" />
		<Attribute name = "transcription_start_site" />
		<Attribute name = "strand" />
	</Dataset>
</Query>""".replace('\n', '').strip()

    def _biomart_query_url(self):
        return self._biomart_endpoint().format(self._xml_query())

    def _fetch_biomart_data(self):

        biomart_url = self._biomart_query_url()

        with temporary_file() as tf_name:
            with open(tf_name, 'w') as tf:
                fetch(biomart_url, tf)

            # Read file and collect lines
            with open(tf_name, 'r') as tf:
                lines = tf.readlines()

        lines = filter(lambda x: x.strip(), lines)  # remove empty lines

        if lines[-1].strip() != '[success]':
            raise Exception('Could not download the binding motifs from Ensembl. '
                            'The completion stamp not present in data.'
                            'Last line: {!r}'.format(lines[-1]))

        lines.pop(-1)  # remove the [success] indicator

        return lines

    def run(self):

        lines = self._fetch_biomart_data()

        columns = map(lambda x: x.split('\t'), lines)
        df = pd.DataFrame(columns, columns=['ensembl_gene_id', 'ensembl_transcript_id',
                                            'transcription_start_site', 'strand'])

        df = df.groupby(['transcription_start_site', 'strand'])[
                        ['ensembl_gene_id', 'ensembl_transcript_id']].apply(
                        lambda x: pd.Series({k: ','.join(x[k]) for k in x}))

        df = df.reset_index().set_index(['ensembl_gene_id', 'ensembl_transcript_id'])

        def int_or_none(x):
            try:
                x = int(x)
            except ValueError:
                x = None
            return x

        df['transcription_start_site'] = df['transcription_start_site'].apply(int_or_none)
        df = df.dropna()
        df['transcription_start_site'] -= 1  # convert to UCSC coordinates





