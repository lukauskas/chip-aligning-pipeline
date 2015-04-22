from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

try:
    from sh import sort, cat, cut
except ImportError:
    sort = None
    cat = None
    cut = None
    raise ImportError('Cannot import sort, cat or cut from the standard unix utilities')