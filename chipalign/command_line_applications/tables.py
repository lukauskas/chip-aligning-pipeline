from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

try:
    from sh import ptrepack
except ImportError:
    ptrepack = None
    raise ImportError('Cannot import ptrepack. Is pytables installed?')