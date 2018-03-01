from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import sh

from chipalign.command_line_applications.exceptions import log_sh_exceptions

try:
    from sh import unzip
    unzip = log_sh_exceptions(unzip)
except ImportError:
    unzip = None
    raise ImportError('Cannot import unzip command from your system, make sure zip archiver is installed')

try:
    from sh import gzip
    gzip = log_sh_exceptions(gzip)
except ImportError:
    gzip = None
    raise ImportError('Cannot import gzip from system.')

seven_z = sh.Command('7z')
seven_z = log_sh_exceptions(seven_z)
