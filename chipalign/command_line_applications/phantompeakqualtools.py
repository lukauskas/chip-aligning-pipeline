from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

# Last tested version
# git commit b9267eaf63895c6330884af241c280c8136e9f11
try:
    from sh import run_spp
except ImportError:
    run_spp = None
    raise ImportError('Cannot import run_spp, ensure phantompeakqualtools is installed https://github.com/kundajelab/phantompeakqualtools')



