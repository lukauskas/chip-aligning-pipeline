from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

try:
    from sh import unzip
except ImportError:
    unzip = None
    raise ImportError('Cannot import zip command from your system, make sure zip archiver is installed')

try:
    from sh import gzip
except ImportError:
    gzip = None
    raise ImportError('Cannot import gzip from system.')