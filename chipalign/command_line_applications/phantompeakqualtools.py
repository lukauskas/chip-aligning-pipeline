from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

# Last tested version
# git commit b9267eaf63895c6330884af241c280c8136e9f11
from chipalign.command_line_applications.exceptions import log_sh_exceptions

try:
    from sh import run_spp
    run_spp = log_sh_exceptions(run_spp)
except ImportError:
    run_spp = None
    raise ImportError('Cannot import run_spp, ensure phantompeakqualtools is installed https://github.com/kundajelab/phantompeakqualtools')



