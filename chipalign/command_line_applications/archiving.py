from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import sh

from chipalign.command_line_applications.exceptions import log_sh_exceptions

try:
    from sh import unzip
    from sh import zip as zip_
    unzip = log_sh_exceptions(unzip)
    zip_ = log_sh_exceptions(zip_)
except ImportError:
    unzip = None
    zip_ = None
    raise ImportError('Cannot import unzip command from your system, make sure zip archiver is installed')

try:
    from sh import gzip, gunzip
    gzip = log_sh_exceptions(gzip)
    gunzip = log_sh_exceptions(gunzip)
except ImportError:
    gzip = None
    gunzip = None
    raise ImportError('Cannot import gzip from system.')

seven_z = sh.Command('7z')
seven_z = log_sh_exceptions(seven_z)

try:
    from sh import tar
    tar = log_sh_exceptions(tar)
except ImportError:
    tar = None
    raise ImportError('Cannot find tar in your system')

