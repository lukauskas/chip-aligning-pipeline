from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

try:
    from sh import run_spp_nodups
except ImportError:
    raise ImportError('Cannot import run_spp_nodups, ensure phantompeakqualtools is installed https://code.google.com/p/phantompeakqualtools/')



