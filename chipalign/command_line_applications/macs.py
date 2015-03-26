from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

try:
    from sh import macs2
except ImportError:
    raise ImportError('Cannot import macs2 from the system, ensure it is installed')
