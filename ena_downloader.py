from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import hashlib
import requests
from task import Task
import luigi
import tempfile
import os
import shutil
from downloader import fetch

_EXPERIMENT_URI_FORMAT = 'https://www.ebi.ac.uk/ena/data/warehouse/filereport?accession={accession}&result=read_run&fields=study_accession,secondary_study_accession,sample_accession,secondary_sample_accession,experiment_accession,experiment_alias,run_accession,tax_id,scientific_name,instrument_model,library_layout,fastq_ftp,fastq_md5'

def _fetch_run_data_for_experiment(experiment_accession, logger=None):
    if logger is None:
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

    cell_type = luigi.Parameter()
    data_track = luigi.Parameter()

    @property
    def parameters(self):
        return [self.cell_type, self.data_track,
                self.study_accession, self.experiment_accession]

    @property
    def _extension(self):
        return 'tar.bz2'

    def run(self):
        logger = self.logger()

        from command_line_applications.archiving import tar

        logger.debug('Fetching runs for {}-{}'.format(self.study_accession, self.experiment_accession))
        runs = _fetch_run_data_for_experiment(self.experiment_accession, logger=logger)
        logger.debug('Got {} runs'.format(len(runs)))

        for run in runs:
            # Validation of data
            if run['study_accession'] != self.study_accession:
                raise ValueError('Run study accession {!r} from ENA does not match study accession {!r}'.format(run['study_accession'], self.study_accession))

        abspath_for_output = os.path.abspath(self.output().path)
        try:
            os.makedirs(os.path.dirname(abspath_for_output))
        except OSError:
            if not os.path.isdir(os.path.dirname(abspath_for_output)):
                raise

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
                with open(run_sequence_filename, 'wb+') as run_sequence_filehandle:
                    fetch(run_url, run_sequence_filehandle, md5_checksum=run['fastq_md5'])

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