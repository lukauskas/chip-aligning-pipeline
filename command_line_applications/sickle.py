from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

try:
    from sh import sickle
except ImportError:
    raise ImportError('Cannot import sickle from your system, make sure it is installed')


