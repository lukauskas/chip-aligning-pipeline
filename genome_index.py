from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import luigi
from os.path import splitext
from genome_browser import GenomeSequence
from task import Task
from downloader import fetch
import os
import logging
import tempfile

def _build_index(twobit_sequence_abspath, name, output, random_seed=0):
    logger = logging.getLogger('genome_index._build_index')

    from command_line_applications.bowtie import bowtie2_build
    from command_line_applications.ucsc_suite import twoBitToFa
    import shutil
    import sh
    import zipfile


    output_abspath = os.path.abspath(output.path)
    output_dir = os.path.dirname(output_abspath)
    logger.debug('Ensuring {} exists'.format(output_dir))
    # Make sure to create directories
    try:
        os.makedirs(output_dir)
    except OSError:
        if not os.path.isdir(output_dir):
            raise

    logging.debug('Building index {} from {}'.format(name, twobit_sequence_abspath))

    current_working_directory = os.getcwdu()

    temporary_directory = tempfile.mkdtemp(prefix='tmp-bt2index-')

    try:
        logger.debug('Changing directory to {}'.format(temporary_directory))
        os.chdir(temporary_directory)

        twobit_sequence_filename = os.path.basename(twobit_sequence_abspath)
        fasta_sequence_filename = u'.'.join([os.path.splitext(twobit_sequence_filename)[0], 'fa'])
        logger.debug('Converting {} to {}'.format(twobit_sequence_filename, fasta_sequence_filename))
        twoBitToFa(twobit_sequence_abspath, fasta_sequence_filename)

        # The actual index to build
        logger.debug('Building index (takes a while)')
        bowtie2_build('-q', '--seed', random_seed, fasta_sequence_filename, name)
        logger.debug('Done building index')
        logger.debug('Files in directory: {}'.format('; '.join(os.listdir('.'))))

        final_filename = '{}.zip'.format(name)

        index_extensions = frozenset(['.bt2', '.bt2l'])
        files_to_zip = filter(lambda x: splitext(x)[1] in index_extensions, os.listdir('.'))

        if not files_to_zip:
            raise Exception('Something is wrong: no files found to zip!')

        logger.debug('Zipping to {}'.format(final_filename))
        with zipfile.ZipFile(final_filename, 'w', allowZip64=True,
                             compression=zipfile.ZIP_DEFLATED) as zipf:
            for file_ in files_to_zip:
                logger.debug('Adding {} to archive'.format(file_))
                zipf.write(file_)

        # TODO: for some reason this moves an empty archive in the end, and I cannot figure out why
        logger.debug('Moving {} to {}'.format(final_filename, output_abspath))
        shutil.move(final_filename, output_abspath)
    finally:
        os.chdir(current_working_directory)
        #shutil.rmtree(temporary_directory)

class GenomeIndex(Task):
    """
    Downloads/creates bowtie2 index for the specified genome version

    """
    genome_version = luigi.Parameter()

    _DOWNLOADABLE_INDICES = {'hg18': 'ftp://ftp.ccb.jhu.edu/pub/data/bowtie2_indexes/hg18.zip',
                             'hg19': 'ftp://ftp.ccb.jhu.edu/pub/data/bowtie2_indexes/hg19.zip'}

    @property
    def parameters(self):
        return [self.genome_version]

    @property
    def _extension(self):
        return 'zip'

    @property
    def _genome_sequence_task(self):
        return GenomeSequence(genome_version=self.genome_version)

    def requires(self):
        if self.genome_version not in self._DOWNLOADABLE_INDICES:
            return self._genome_sequence_task

    def run(self):
        if self.genome_version in self._DOWNLOADABLE_INDICES:
            with self.output().open('w') as output_file:
                fetch(self._DOWNLOADABLE_INDICES[self.genome_version], output_file)
        elif self.genome_version in self._GENOME_SEQUENCES:
            sequence_filename = os.path.abspath(self._genome_sequence_task.output().path)
            with self.output().open('w') as output_file:
                _build_index(sequence_filename, self.genome_version, output_file)
        else:
            raise ValueError('Unsupported genome version: {0!r}'.format(self.genome_version))


if __name__ == '__main__':
    logging.getLogger('genome_index._build_index').setLevel(logging.DEBUG)
    luigi.run()