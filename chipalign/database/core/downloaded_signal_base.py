import luigi
import os
import shutil

from chipalign.core.downloader import fetch
from chipalign.core.file_formats.bedgraph import BedGraph
from chipalign.core.task import Task


def _bigwig_to_bedgraph(bigwig_filename, output_filename, logger=None):
    """
    Convert bigwig file to a bedgraph file

    :param bigwig_filename:
    :param output_filename:
    :param logger:
    :return:
    """
    from chipalign.command_line_applications.ucsc_suite import bigWigToBedGraph
    from chipalign.command_line_applications.common import sort
    from chipalign.command_line_applications.archiving import gzip as cmd_line_gzip

    if logger is None:
        import logging
        logger = logging.getLogger('chipalign.database.core.downloaded_signal_base.bigwig_to_bedgraph')

    logger.info('Converting to bedgraph')
    tmp_bedgraph = 'download.bedgraph'
    bigWigToBedGraph(bigwig_filename, tmp_bedgraph)
    logger.info('Sorting')
    filtered_sorted_bedgraph = 'filtered.sorted.bedgraph'
    # GNU sort should be faster, use less memory and generally more stable than pybedtools
    sort(tmp_bedgraph, '-k1,1', '-k2,2n', '-k3,3n', '-k5,5n',
         '-o', filtered_sorted_bedgraph)
    # Free up some /tmp/ space
    os.unlink(tmp_bedgraph)
    logger.info('Gzipping')
    cmd_line_gzip('-9', filtered_sorted_bedgraph)
    filtered_and_sorted = 'filtered.sorted.bedgraph.gz'
    logger.info('Moving')
    shutil.move(filtered_and_sorted, output_filename)


class DownloadedSignalBase(Task):
    """
    A base class for downloading signal in bigWig format and converting it to bedgraph format.
    """

    @property
    def task_class_friendly_name(self):
        return 'DSignal'

    def url(self):
        raise NotImplementedError()

    @property
    def _extension(self):
        return 'bdg.gz'

    @property
    def _output_class(self):
        return BedGraph

    def run(self):
        logger = self.logger()
        url = self.url()

        output_abspath = os.path.abspath(self.output().path)
        self.ensure_output_directory_exists()

        with self.temporary_directory():
            logger.info('Fetching: {}'.format(url))
            filename = 'download.bigwig'
            with open(filename, 'wb') as f:
                fetch(url, f)

            _bigwig_to_bedgraph(filename, output_abspath, logger=logger)

