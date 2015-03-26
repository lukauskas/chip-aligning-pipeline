from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import gzip
from itertools import imap
import os
import shutil

from task import Task

from genome.genome_alignment import ConsolidatedReads
from core.file_formats.yaml_file import YamlFile


class CrossCorrelationPlot(Task):

    input_task = luigi.Parameter()

    def requires(self):
        return self.input_task

    @property
    def _extension(self):
        return 'pdf'

    @property
    def parameters(self):
        return self.input_task.parameters

    def run(self):
        from command_line_applications.phantompeakqualtools import run_spp_nodups
        logger = self.logger()

        input_abspath = os.path.abspath(self.input().path)
        output_abspath = os.path.abspath(self.output().path)
        self.ensure_output_directory_exists()

        with self.temporary_directory():
            tmp_output = 'output.tmp'
            logger.debug('Running SPP')
            run_spp_nodups('-c={}'.format(input_abspath),
                           '-savp={}'.format(tmp_output),
                           '-odir=.')

            logger.debug('Relocating output')
            shutil.move(tmp_output, output_abspath)


class FragmentLength(Task):

    input_task = luigi.Parameter()

    def requires(self):
        return self.input_task

    def output(self):
        super_output = super(FragmentLength, self).output().path
        return YamlFile(super_output)

    @property
    def parameters(self):
        return self.input_task.parameters

    @property
    def _extension(self):
        return 'yml'

    def run(self):
        from command_line_applications.phantompeakqualtools import run_spp_nodups
        logger = self.logger()

        input_abspath = os.path.abspath(self.input().path)
        output = self.output()

        with self.temporary_directory():
            tmp_output = 'output.tmp'
            logger.debug('Running SPP')
            run_spp_nodups('-c={}'.format(input_abspath),
                           '-out={}'.format(tmp_output),
                           '-odir=.')

            logger.debug('Parsing output')
            with open(tmp_output) as f:
                data = f.read()

        data = data.strip().split('\t')

        filename, num_reads, estimated_fragment_length, corr_estimated_fragment_length, \
        phantom_peak, corr_phantom_peak, argmin_corr, min_corr, nsc, rsc, quality_tag = data

        estimated_fragment_length = map(int, estimated_fragment_length.split(','))
        corr_estimated_fragment_length = map(float, corr_estimated_fragment_length.split(','))
        assert len(estimated_fragment_length) == len(corr_estimated_fragment_length)

        fragment_lengths = [{'length': x, 'correlation': y}
                            for x, y in zip(estimated_fragment_length, corr_estimated_fragment_length)]

        data_fragment_lengths = {i: v for i, v in enumerate(fragment_lengths)}
        data_fragment_lengths['best'] = data_fragment_lengths[0]

        quality_tag = int(quality_tag)
        quality_tag_map = {-2: 'very low',
                           -1: 'low',
                           0: 'medium',
                           1: 'high',
                           2: 'very high'}

        data = {}
        data['filename'] = filename
        data['num_reads'] = int(num_reads)
        data['fragment_lengths'] = data_fragment_lengths
        data['phantom_peak'] = {'read_length': int(phantom_peak), 'correlation': float(corr_phantom_peak)}
        data['min_strand_shift'] = {'shift': int(argmin_corr), 'correlation': float(min_corr)}
        data['nsc'] = float(nsc)
        data['rsc'] = float(rsc)
        data['quality_tag'] = {'numeric': quality_tag, 'text': quality_tag_map[quality_tag]}

        logger.debug('Writing output')
        output.dump(data)

class Signal(Task):

    input_task = luigi.Parameter()
    treatment_task = luigi.Parameter()

    @property
    def fragment_length_task(self):
        return FragmentLength(self.treatment_task)

    def requires(self):
        return [self.input_task, self.treatment_task, self.fragment_length_task]

    @property
    def parameters(self):
        return [self.input_task.__class__.__name__] + self.input_task.parameters \
               + [self.treatment_task.__class__.__name__] + self.treatment_task.parameters

    @property
    def _extension(self):
        return 'bdg.gz'

    def run(self):
        from command_line_applications.macs import macs2
        import pybedtools

        logger = self.logger()

        fragment_length = self.fragment_length_task.output().load()['fragment_lengths']['best']['length']
        ext_size = int(fragment_length/2)

        logger.debug('Fragment length: {}, ext_size: {}'.format(fragment_length, ext_size))

        treatment_abspath = os.path.abspath(self.treatment_task.output().path)
        input_abspath = os.path.abspath(self.input_task.output().path)

        number_of_treatment_reads = len(pybedtools.BedTool(treatment_abspath))
        number_of_input_reads = len(pybedtools.BedTool(input_abspath))

        logger.debug('Number of reads. Treatment: {}, input: {}'.format(number_of_treatment_reads, number_of_input_reads))

        scaling_factor = min(number_of_treatment_reads, number_of_input_reads) / 1000000.0
        logger.debug('Estimated scaling factor: {}'.format(scaling_factor))

        output_abspath = os.path.abspath(self.output().path)
        self.ensure_output_directory_exists()

        with self.temporary_directory():

            logger.debug('Running MACS callpeak')
            output_basename = 'macs_out'

            macs2('callpeak',
                  '--nomodel',
                  '--extsize', ext_size,
                  '-B',
                  '--SPMR',
                  f='BED',
                  t=treatment_abspath,
                  c=input_abspath,
                  g='hs',
                  n=output_basename,
                  p=1e-2,
                  )

            logger.debug('Now running bdgcmp')
            pval_signal_output = 'pval.signal'
            macs2('bdgcmp',
                  t='{}_treat_pileup.bdg'.format(output_basename),
                  c='{}_control_lambda.bdg'.format(output_basename),
                  o=pval_signal_output,
                  m='ppois',
                  S=scaling_factor
                  )

            logger.debug('Sorting the output')
            pval_signal_bedtool = pybedtools.BedTool(pval_signal_output)
            pval_signal_bedtool = pval_signal_bedtool.sort()

            try:
                logger.debug('Gzipping')
                tmp_gzip_file = 'output.gz'
                with gzip.GzipFile(tmp_gzip_file, 'w') as out_:
                    out_.writelines(imap(str, pval_signal_bedtool))
                logger.debug('Moving')
                shutil.move(tmp_gzip_file, output_abspath)
            finally:
                pval_signal_bedtool_fn = pval_signal_bedtool.fn
                del pval_signal_bedtool
                try:
                    os.unlink(pval_signal_bedtool_fn)
                except OSError:
                    if os.path.isfile(pval_signal_bedtool_fn):
                        raise

if __name__ == '__main__':
    import logging
    logging.basicConfig()
    FragmentLength.class_logger().setLevel(logging.DEBUG)
    import luigi
    filtered_reads_task = ConsolidatedReads(genome_version='hg19',
                                            aligner='pash',
                                            srr_identifiers=['SRR179694', 'SRR097968'],
                                            chromosomes='female',
                                            max_sequencing_depth=30000000)

    task = FragmentLength(input_task=filtered_reads_task)
    task2 = CrossCorrelationPlot(input_task=filtered_reads_task)

    _sch = luigi.interface.WorkerSchedulerFactory().create_local_scheduler()
    _w = luigi.worker.Worker(scheduler=_sch)
    _w.add(task)
    _w.add(task2)
    _w.run()




