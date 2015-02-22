from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import luigi
from task import Task
from downloader import fetch
import os
import logging
import tempfile

def _build_index(url_of_2bit_sequence, name, output, random_seed=0):
    logger = logging.getLogger('genome_index._build_index')

    from command_line_applications.bowtie import bowtie2_build
    from command_line_applications.ucsc_suite import twoBitToFa
    from command_line_applications.archiving import zip
    import shutil
    import sh

    output_abspath = os.path.abspath(output.path)
    output_dir = os.path.dirname(output_abspath)
    logger.debug('Ensuring {} exists'.format(output_dir))
    # Make sure to create directories
    try:
        os.makedirs(output_dir)
    except OSError:
        if not os.path.isdir(output_dir):
            raise

    logging.debug('Building index {} from {}'.format(name, url_of_2bit_sequence))

    current_working_directory = os.getcwdu()

    temporary_directory = tempfile.mkdtemp(prefix='tmp-bt2index-')

    try:
        logger.debug('Changing directory to {}'.format(temporary_directory))
        os.chdir(temporary_directory)

        twobit_sequence_filename = os.path.basename(url_of_2bit_sequence)
        logger.debug('Fetching: {}'.format(url_of_2bit_sequence))
        with open(twobit_sequence_filename, 'wb') as sequence_filehandle:
            fetch(url_of_2bit_sequence, sequence_filehandle)
        logger.debug('Fetching: {} done'.format(url_of_2bit_sequence))

        fasta_sequence_filename = u'.'.join([os.path.splitext(twobit_sequence_filename)[0], 'fa'])
        logger.debug('Converting {} to {}'.format(twobit_sequence_filename, fasta_sequence_filename))
        twoBitToFa(twobit_sequence_filename, fasta_sequence_filename)

        logger.debug('Removing {}'.format(twobit_sequence_filename))
        os.remove(twobit_sequence_filename)
        del twobit_sequence_filename  # mark the variable as deleted so we do not accidentally refer to the filename again

        # The actual index to build
        logger.debug('Building index (takes a while)')
        bowtie2_build('-q', '--seed', random_seed, fasta_sequence_filename, name)
        logger.debug('Done building index')
        logger.debug('Files in directory: {}'.format('; '.join(os.listdir('.'))))

        final_filename = '{}.zip'.format(name)

        if os.path.exists('{}.1.bt2'.format(name)):
            # small index
            wildcard = '{}*.bt2'.format(name)
        elif os.path.exists('{}.1.bt2l'.format(name)):
            # Large index
            wildcard = '{}*.bt2l'.format(name)
        else:
            raise Exception('Cannot determine wildcard for index')
        logger.debug('Zipping {} to {}'.format(wildcard, final_filename))
        zip(final_filename, sh.glob(wildcard))

        logger.debug('Moving {} to {}'.format(final_filename, output_abspath))
        shutil.move(final_filename, output_abspath)
    finally:
        os.chdir(current_working_directory)
        shutil.rmtree(temporary_directory)

class GenomeIndex(Task):
    """
    Downloads/creates bowtie2 index for the specified genome version

    """
    genome_version = luigi.Parameter()

    _DOWNLOADABLE_INDICES = {'hg18': 'ftp://ftp.ccb.jhu.edu/pub/data/bowtie2_indexes/hg18.zip',
                             'hg19': 'ftp://ftp.ccb.jhu.edu/pub/data/bowtie2_indexes/hg19.zip'}

    _GENOME_SEQUENCES = {'hg38': 'http://hgdownload.cse.ucsc.edu/goldenPath/hg38/bigZips/hg38.2bit'}

    @property
    def _parameters(self):
        return [self.genome_version]

    @property
    def _extension(self):
        return 'zip'

    def run(self):
        if self.genome_version in self._DOWNLOADABLE_INDICES:
            with self.output().open('w') as output_file:
                fetch(self._DOWNLOADABLE_INDICES[self.genome_version], output_file)
        elif self.genome_version in self._GENOME_SEQUENCES:
            with self.output().open('w') as output_file:
                _build_index(self._GENOME_SEQUENCES[self.genome_version], self.genome_version, output_file)
        else:
            raise ValueError('Unsupported genome version: {0!r}'.format(self.genome_version))


if __name__ == '__main__':
    logging.getLogger('genome_index._build_index').setLevel(logging.DEBUG)
    luigi.run()