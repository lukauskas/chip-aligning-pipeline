from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from chipalign.command_line_applications.exceptions import log_sh_exceptions

try:
    from sh import pash3
    pash3 = log_sh_exceptions(pash3)
except ImportError:
    pash3 = None
    raise ImportError('Cannot import Pash3, ensure pash is installed')