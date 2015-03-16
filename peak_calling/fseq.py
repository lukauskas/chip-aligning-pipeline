from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import logging
import os
import luigi
import shutil
from peak_calling.base import PeaksBase
from util import temporary_directory


class FseqPeaks(PeaksBase):

    def run(self):

        from command_line_applications.fseq import fseq
        from command_line_applications.bedtools import bamToBed
        logger = self.logger()

        input_file_abspath = os.path.abspath(self.alignment_task.output()[0].path)
        bed_output, stdout_output = self.output()

        bed_output_abspath = os.path.abspath(bed_output.path)
        stdout_output_abspath = os.path.abspath(stdout_output.path)

        try:
            os.makedirs(os.path.dirname(bed_output_abspath))
        except OSError:
            if not os.path.isdir(os.path.dirname(bed_output_abspath)):
                raise

        try:
            os.makedirs(os.path.dirname(stdout_output_abspath))
        except OSError:
            if not os.path.isdir(os.path.dirname(stdout_output_abspath)):
                raise

        with temporary_directory(prefix='fseq-peaks', cleanup_on_exception=False, logger=logger):
            logger.debug('Converting bam to bed')
            alignments_bed = 'alignments.bed'
            bamToBed('-i', input_file_abspath, _out=alignments_bed)

            logger.debug('Creating output directory for fseq')
            stdout_file = 'fseq.stdout'
            output_dir = 'output'
            try:
                os.makedirs(output_dir)
            except OSError:
                if not os.path.isdir(output_dir):
                    raise
            logger.debug('Running fseq')
            fseq('-f', 0,
                 '-o', output_dir,
                 '-of', 'npf',
                 alignments_bed,
                 '-v', _out=stdout_file)

            logger.debug('Joining output npfs to one')
            answer_bed = 'answer.bed'
            files_to_join = filter(lambda x: os.path.splitext(x)[1] == '.npf', os.listdir(output_dir))
            number_of_files_to_join = len(files_to_join)
            if number_of_files_to_join == 0:
                raise Exception('Zero npf files output by fseq')

            with open(answer_bed, 'w') as answer:
                for i, filename in enumerate(files_to_join, start=1):
                    logger.debug('Joining {}/{}: {}'.format(i, number_of_files_to_join, filename))

                    filename = os.path.join(output_dir, filename)
                    with open(filename, 'r') as input_:
                        answer.writelines(input_)

            logger.debug('Moving output to the final directory')
            shutil.move(stdout_file, stdout_output_abspath)
            shutil.move(answer_bed, bed_output_abspath)

if __name__ == '__main__':
    FseqPeaks.logger().setLevel(logging.DEBUG)
    logging.basicConfig()
    luigi.run(main_task_cls=FseqPeaks)
