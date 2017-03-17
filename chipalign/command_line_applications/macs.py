from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from chipalign.command_line_applications.exceptions import log_sh_exceptions

try:
    from sh import macs2
    macs2 = log_sh_exceptions(macs2)
except ImportError:
    macs2 = None
    raise ImportError('Cannot import macs2 from the system, ensure it is installed')
