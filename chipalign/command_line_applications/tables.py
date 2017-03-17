from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from chipalign.command_line_applications.exceptions import log_sh_exceptions

try:
    from sh import ptrepack
    ptrepack = log_sh_exceptions(ptrepack)
except ImportError:
    ptrepack = None
    raise ImportError('Cannot import ptrepack. Is pytables installed?')

try:
    from sh import h5repack
    h5repack = log_sh_exceptions(h5repack)
except ImportError:
    h5repack = None
    raise ImportError('Cannot import h5repack. Is h5 library installed?')