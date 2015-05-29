from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from unittest import TestCase
import tempfile
import os
import shutil
import luigi
from chipalign.core.downloader import fetch

from chipalign.core.util import _CHIPALIGN_OUTPUT_DIRECTORY_ENV_VAR, temporary_file


class TaskTestCase(TestCase):

    def setUp(self):
        self.__temporary_output_directory = tempfile.mkdtemp(prefix='tests-temp')
        os.environ[_CHIPALIGN_OUTPUT_DIRECTORY_ENV_VAR] = self.__temporary_output_directory

    def tearDown(self):
        try:
            shutil.rmtree(self.__temporary_output_directory)
        except OSError:
            if os.path.isdir(self.__temporary_output_directory):
                raise

    def build_task(self, task):
        luigi.build([task], local_scheduler=True)
        self.assertTrue(task.complete())

    @classmethod
    def task_cache_directory(cls, make_if_not_exists=True):
        """
        Returns the directory for cache
        :param make_if_not_exists:
        :return:
        """
        absfile = os.path.abspath(__file__)
        dirname = os.path.dirname(absfile)

        class_name = cls.__name__

        dirname = os.path.join(dirname, '.cache-{}'.format(class_name))
        if make_if_not_exists and not os.path.isdir(dirname):
            os.makedirs(dirname)

        return dirname
