import os
import sh

from chipalign.command_line_applications.exceptions import log_sh_exceptions

try:
    from sh import bwa as bwa
    bwa = log_sh_exceptions(bwa)
except ImportError:
    bwa = None
    raise ImportError('Cannot find BWA executable in the user\'s system. '
                      'Please install BWA using your system\'s package manager')


from chipalign.command_line_applications.sh_proxy import sh_proxy
bwa_proxied = log_sh_exceptions(sh_proxy.bwa)


_this_dir = os.path.dirname(os.path.abspath(__file__))
_shell_file = os.path.join(_this_dir, 'bwa.sh')
bwa_piped = sh.Command(_shell_file)
