from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from chipalign.command_line_applications.exceptions import log_sh_exceptions

try:
    from sh import sort, cat, cut
    sort = log_sh_exceptions(sort)
    cat = log_sh_exceptions(cat)
    cut = log_sh_exceptions(cut)
except ImportError:
    sort = None
    cat = None
    cut = None
    raise ImportError('Cannot import sort, cat or cut from the standard unix utilities')