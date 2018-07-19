from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from chipalign.command_line_applications.exceptions import log_sh_exceptions

try:
    from sh import bwa as bwa_
except ImportError:
    bwa = None
    raise ImportError('Cannot find BWA executable in the user\'s system. '
                      'Please install BWA using your system\'s package manager')


from chipalign.command_line_applications.sh_proxy import sh_proxy
bwa = log_sh_exceptions(sh_proxy.bwa)

