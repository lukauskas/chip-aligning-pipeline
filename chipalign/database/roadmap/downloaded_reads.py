from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import gzip
import os
import shutil
import luigi
from chipalign.core.downloader import fetch
from chipalign.core.task import Task
from chipalign.core.util import autocleaning_pybedtools


class RoadmapAlignedReads(Task):
    """
    Downloads aligned reads from `ROADMAP`_. This task is designed to be a
    drop-in replacement for :class:`~chipalign.alignment.consolidation.AlignedSRR` task.

    :param url: url to download
    :param genome_version: genome version to use

    .. _ROADMAP: http://egg2.wustl.edu/roadmap/web_portal/processed_data.html
    """

    url = luigi.Parameter()
    genome_version = luigi.Parameter()

    @property
    def _extension(self):
        return 'bam'

    @property
    def parameters(self):
        file_base, __ = os.path.splitext(os.path.basename(self.url))
        return [file_base]

    def _run(self):
        output_abspath = os.path.abspath(self.output().path)
        self.ensure_output_directory_exists()

        with self.temporary_directory():

            tmp_file = 'downloaded.bed.gz'

            with open(tmp_file, 'wb') as tf:
                fetch(self.url, tf)

            # Their BED files are broken, to fix:
            # adjust start coordinate by one to the left, irrespective of strand
            # adjust the strand column to column 6

            fixed_bed = 'fixed.bed'

            with gzip.open(tmp_file, 'r') as _in:
                with open(fixed_bed, 'w') as _out:
                    for row in _in:
                        chrom, start, end, name, strand = row.split('\t')

                        start = int(start) - 1  # Yup, one subtracted here intentionally
                        end = int(end)

                        score = '.'  # No score

                        _out.write('{}\t{}\t{}\t{}\t{}\t{}\n'.format(chrom, start, end, name, score, strand))

            # Now one needs to convert the bed to BAM
            tmp_bam = 'temporary.bam'
            with autocleaning_pybedtools() as pybedtools:
                pybedtools.BedTool(fixed_bed).to_bam(genome=self.genome_version).saveas(tmp_bam)

                # Finally, move the bam
                shutil.move(tmp_bam, output_abspath)

    def bam_output(self):
        return self.output()
