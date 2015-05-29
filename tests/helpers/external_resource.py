from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import shutil
from chipalign.core.downloader import fetch
from chipalign.core.util import temporary_file
class ExternalResource(object):

    associated_task_test_case = None

    def __init__(self, associated_task_test_case):
        self.associated_task_test_case = associated_task_test_case

        try:
            __ = associated_task_test_case.task_cache_directory()
        except AttributeError as e:
            raise AttributeError('Task should have task_cache_directory function: {!r}'.format(e))

    def _directory(self):
        return self.associated_task_test_case.task_cache_directory()

class DownloadableExternalResource(ExternalResource):

    url = None

    def __init__(self, associated_task_test_case, url):
        super(DownloadableExternalResource, self).__init__(associated_task_test_case)
        self.url = url

    def _filename(self):
        final_location = os.path.join(self._directory(),
                                      '{}-cache'.format(os.path.basename(self.url)))

        return final_location

    def _fetch_resource(self, temp_file):
        with open(temp_file) as tf:
            fetch(self.url, tf)

    def _relocate_to_output(self, temp_file):
        shutil.move(temp_file, self._filename())

    def exists(self):
        return os.path.isfile(self._filename())

    def _obtain(self):
        with temporary_file(cleanup_on_exception=True) as temp_file:
            self._fetch_resource(temp_file)
            self._relocate_to_output(temp_file)

    def get(self):
        if not self.exists():
            self._obtain()
        return self._filename()