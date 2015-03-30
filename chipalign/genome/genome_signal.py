from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import gzip
import os
import shutil

import luigi
import tarfile

from chipalign.core.task import Task
from chipalign.genome.peaks import MACSResults


class Signal(Task):

    input_task = luigi.Parameter()
    treatment_task = luigi.Parameter()

    fragment_length = luigi.Parameter(default='auto')
    scaling_factor = luigi.Parameter(default='auto')

    @property
    def macs_task(self):
        return MACSResults(input_task=self.input_task, treatment_task=self.treatment_task,
                           fragment_length=self.fragment_length)

    def requires(self):
        return self.macs_task

    @property
    def parameters(self):
        additional_parameters = []
        if self.scaling_factor != 'auto':
            additional_parameters.append('sf{}'.format(self.scaling_factor))

        return self.requires().parameters + additional_parameters

    @property
    def _extension(self):
        return 'bdg.gz'

    def scaling_factor_value(self):
        import pybedtools
        logger = self.logger()


        if self.scaling_factor == 'auto':
            number_of_treatment_reads = len(pybedtools.BedTool(self.treatment_task.output().path))
            number_of_input_reads = len(pybedtools.BedTool(self.input().output().path))

            logger.debug('Number of reads. Treatment: {}, input: {}'.format(number_of_treatment_reads,
                                                                            number_of_input_reads))

            scaling_factor = min(number_of_treatment_reads, number_of_input_reads) / 1000000.0
            logger.debug('Estimated scaling factor: {}'.format(scaling_factor))

        else:
            scaling_factor = float(self.scaling_factor)
            logger.debug('Using user-defined scaling factor: {}'.format(scaling_factor))

        return scaling_factor

    def run(self):
        from chipalign.command_line_applications.macs import macs2

        logger = self.logger()

        macs_callpeaks_files_abspath = os.path.abspath(self.macs_task.output().path)

        scaling_factor = self.scaling_factor_value()

        output_abspath = os.path.abspath(self.output().path)
        self.ensure_output_directory_exists()


        with self.temporary_directory():

            macs_basename = MACSResults.OUTPUT_BASENAME
            treat_pileup_filename = '{}_treat_pileup.bdg'.format(macs_basename)
            control_lambda_filename = '{}_control_lambda.bdg'.format(macs_basename)

            logger.debug('Extracting files')
            with tarfile.open(macs_callpeaks_files_abspath, 'r') as tf:
                tf.extract(treat_pileup_filename)
                tf.extract(control_lambda_filename)

            logger.debug('Now running bdgcmp')
            pval_signal_output = 'pval.signal'
            macs2('bdgcmp',
                  t=treat_pileup_filename,
                  c=control_lambda_filename,
                  o=pval_signal_output,
                  m='ppois',
                  S=scaling_factor
                  )

            logger.info('Writing the output in a sorted order')
            # MACS returns sorted signal output, but the chromosomes are in random order
            # Let's fix that
            chromosomes = set()
            with open(pval_signal_output, 'r') as f:
                for row in f:
                    chrom, __, __ = row.partition('\t')
                    chromosomes.add(chrom)

            sorted_chromosomes = sorted(chromosomes)

            tmp_gzip_file = 'output.gz'
            with gzip.GzipFile(tmp_gzip_file, 'w') as out_:
                for chrom in sorted_chromosomes:
                    logger.info('Processing chromosome: {}'.format(chrom))

                    # This loops through the input file, and writes it to out_ file chromosome by chromosome as defined
                    # in sorted_chromosomes
                    with open(pval_signal_output, 'r') as in_:
                        seen_chrom = False
                        for row in in_:
                            in_chrom, __, __ = row.partition('\t')

                            # If chromosomes match, write it
                            if chrom == in_chrom:
                                out_.write(row)
                                seen_chrom = True
                            # Else either stop (if we already processed chromosome required)
                            # .. or continue looking for it
                            else:
                                if seen_chrom:
                                    break
                                else:
                                    continue

            logger.info('Moving')
            shutil.move(tmp_gzip_file, output_abspath)

            logger.info('Done')