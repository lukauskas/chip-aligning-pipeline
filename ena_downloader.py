from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import requests
from task import Task
import luigi
import tempfile
import os
import shutil
from downloader import fetch

_EXPERIMENT_URI_FORMAT = 'https://www.ebi.ac.uk/ena/data/warehouse/filereport?accession={accession}&result=read_run&fields=study_accession,secondary_study_accession,sample_accession,secondary_sample_accession,experiment_accession,experiment_alias,run_accession,tax_id,scientific_name,instrument_model,library_layout,fastq_ftp'

def _fetch_run_data_for_experiment(experiment_accession):
    logger = logging.getLogger('ena_downloader.fetch_runs')
    uri = _EXPERIMENT_URI_FORMAT.format(accession=experiment_accession)
    logger.debug('Fetching {}'.format(uri))

    response = requests.get(uri)
    text = response.text

    lines = text.split('\n')

    headers = lines.pop(0).split('\t')

    runs_dict = [dict(zip(headers, line.split('\t'))) for line in lines if line.strip()]

    # Sanity check
    for run in runs_dict:
        assert run['experiment_accession'] == experiment_accession

    if not runs_dict:
        raise Exception('No runs found for experiment accession {!r}'.format(experiment_accession))

    return runs_dict

class ShortReadsForExperiment(Task):

    experiment_accession = luigi.Parameter()
    study_accession = luigi.Parameter()
    experiment_alias = luigi.Parameter()

    @property
    def parameters(self):
        return [self.study_accession, self.experiment_accession, self.experiment_alias]

    @property
    def _extension(self):
        return 'tar.bz2'

    def run(self):
        logger = ShortReadsForExperiment.logger()

        from command_line_applications.archiving import tar

        logger.debug('Fetching runs for {}'.format(self.experiment_accession))
        runs = _fetch_run_data_for_experiment(self.experiment_accession)
        logger.debug('Got {} runs'.format(len(runs)))

        for run in runs:
            # Validation of data
            if run['study_accession'] != self.study_accession:
                raise ValueError('Run study accession {!r} from ENA does not match study accession {!r}'.format(run['study_accession'], self.study_accession))
            if run['experiment_alias'] != self.experiment_alias:
                raise ValueError('Run experiment alias {!r} from ENA does not match experiment alias {!r}'.format(run['experiment_alias'], self.experiment_alias))

        abspath_for_output = os.path.abspath(self.output().path)

        current_directory = os.getcwdu()
        temp_directory = tempfile.mkdtemp()

        try:
            os.chdir(temp_directory)
            logging.debug('Working in {}'.format(temp_directory))

            run_sequence_filenames = []
            for run in runs:
                run_accession = run['run_accession']
                run_sequence_filename = '.'.join([self._basename, run_accession, 'fastq.gz'])

                run_url = 'ftp://' + run['fastq_ftp']
                logger.debug('Fetching: {} to {}'.format(run_url, run_sequence_filename))
                with open(run_sequence_filename, 'w') as run_sequence_filehandle:
                    fetch(run_url, run_sequence_filehandle)

                run_sequence_filenames.append(run_sequence_filename)

            archive_name = 'sequences.tar.bz2'
            logger.debug('Compressing to {}'.format(archive_name))
            tar('-jcf', archive_name, *run_sequence_filenames)
            logger.debug('Moving to {}'.format(abspath_for_output))
            shutil.move(archive_name, abspath_for_output)
        finally:
            os.chdir(current_directory)
            shutil.rmtree(temp_directory)


if __name__ == '__main__':
    ShortReadsForExperiment.logger().setLevel(logging.DEBUG)
    logging.basicConfig()
    luigi.run(main_task_cls=ShortReadsForExperiment)