from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import luigi
from chipalign.core.file_formats.yaml_file import YamlFile
from chipalign.core.task import Task
from chipalign.core.util import timed_segment


class FragmentLength(Task):
    """
        Extracts the fragment length information using `SPP pipeline`_.

        :param input_task: input task.
        :type input_task: Generally a :class:`~chipalign.alignment.consolidation.ConsolidatedReads` task

        .. _SPP pipeline: https://github.com/kundajelab/phantompeakqualtools
    """

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

    def _run(self):
        from chipalign.command_line_applications.phantompeakqualtools import run_spp
        logger = self.logger()

        input_abspath = os.path.abspath(self.input().path)
        output = self.output()

        with self.temporary_directory():
            tmp_output = 'output.tmp'
            with timed_segment('Running SPP'):
                run_spp('-c={}'.format(input_abspath),
                        '-out={}'.format(tmp_output),
                        '-odir=.')

            logger.debug('Parsing output')
            with open(tmp_output) as f:
                data = f.read()

        data = data.strip().split('\t')

        filename, num_reads, estimated_fragment_length, corr_estimated_fragment_length, \
        phantom_peak, corr_phantom_peak, argmin_corr, min_corr, nsc, rsc, quality_tag = data

        estimated_fragment_length = list(map(int, estimated_fragment_length.split(',')))
        corr_estimated_fragment_length = list(map(float, corr_estimated_fragment_length.split(',')))
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
