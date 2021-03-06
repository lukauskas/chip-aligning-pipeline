from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import gzip
import os
import shutil
import tarfile

import luigi

from chipalign.core.file_formats.bedgraph import BedGraph
from chipalign.core.task import Task
from chipalign.core.util import autocleaning_pybedtools, timed_segment
from chipalign.signal.peaks import MACSResults

class Signal(Task):
    """
    Creates a bedgraph of signal track (p-values) as specified by ROADMAP pipeline

    :param input_task: Consolidated reads for input
    :param treatment_task: Consolidated reads for treatment
    :param fragment_length: fragment length to use (`'auto'` to estimate it automatically)
    :param scaling_factor: float scaling factor to use (`'auto'` to automatically estimate from the data)
    """
    input_task = luigi.Parameter()
    treatment_task = luigi.Parameter()

    fragment_length = luigi.Parameter(default='auto')
    scaling_factor = luigi.Parameter(default='auto')

    @property
    def macs_task(self):
        return MACSResults(input_task=self.input_task,
                           treatment_task=self.treatment_task,
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

    @property
    def _output_class(self):
        return BedGraph

    def scaling_factor_value(self):
        with autocleaning_pybedtools() as pybedtools:
            logger = self.logger()
            logger.debug('Input task: {!r} (output: {!r})'.format(self.input_task, self.input_task.output()))
            logger.debug('Treatment task: {!r} (output {!r})'.format(self.treatment_task, self.treatment_task.output()))
            if self.scaling_factor == 'auto':
                number_of_treatment_reads = pybedtools.BedTool(self.treatment_task.output().path).count()
                number_of_input_reads = pybedtools.BedTool(self.input_task.output().path).count()

                logger.debug('Number of reads. Treatment: {}, input: {}'.format(number_of_treatment_reads,
                                                                                number_of_input_reads))
                assert number_of_input_reads > 0 and number_of_input_reads > 0

                scaling_factor = min(number_of_treatment_reads, number_of_input_reads) / 1000000.0
                logger.debug('Estimated scaling factor: {}'.format(scaling_factor))

            else:
                scaling_factor = float(self.scaling_factor)
                logger.debug('Using user-defined scaling factor: {}'.format(scaling_factor))

            return scaling_factor

    def _run(self):
        from chipalign.command_line_applications.macs import macs2
        from chipalign.command_line_applications.ucsc_suite import bedClip
        from chipalign.command_line_applications.archiving import seven_z

        logger = self.logger()

        macs_callpeaks_files_abspath = os.path.abspath(self.macs_task.output().path)

        scaling_factor = self.scaling_factor_value()

        output_abspath = os.path.abspath(self.output().path)
        self.ensure_output_directory_exists()

        with self.temporary_directory():

            macs_basename = MACSResults.OUTPUT_BASENAME
            treat_pileup_filename = '{}_treat_pileup.bdg'.format(macs_basename)
            control_lambda_filename = '{}_control_lambda.bdg'.format(macs_basename)

            with timed_segment('Extracting MACS2 result'):
                seven_z('x', macs_callpeaks_files_abspath, treat_pileup_filename,
                        control_lambda_filename)

            with timed_segment('Running MACS2 bdgcmp'):
                pval_signal_output_raw = 'pval.unclipped.signal'
                macs2('bdgcmp',
                      t=treat_pileup_filename,
                      c=control_lambda_filename,
                      o=pval_signal_output_raw,
                      m='ppois',
                      S=scaling_factor
                      )

            with autocleaning_pybedtools() as pybedtools:
                with timed_segment('Clipping result with pybedtools'):
                    tmp_bedtool = pybedtools.BedTool(pval_signal_output_raw).truncate_to_chrom(
                        genome=self.treatment_task.genome_version)
                    tmp_bedtool.saveas(pval_signal_output_raw)

                chromsizes_file = 'chromsizes'
                pybedtools.chromsizes_to_file(self.treatment_task.genome_version, chromsizes_file)

            with timed_segment('Running bedClip'):
                pval_signal_output = 'pval.signal'
                bedClip(pval_signal_output_raw, chromsizes_file, pval_signal_output)
                os.unlink(pval_signal_output_raw)

            with timed_segment('Writing MACS output in sorted order'):
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
                        # This loops through the input file, and writes it to out_ file chromosome by chromosome as defined
                        # in sorted_chromosomes
                        with open(pval_signal_output, 'rb') as in_:
                            seen_chrom = False
                            for row in in_:
                                in_chrom, __, __ = row.partition(b'\t')
                                in_chrom = in_chrom.decode('ascii')

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

                shutil.move(tmp_gzip_file, output_abspath)
