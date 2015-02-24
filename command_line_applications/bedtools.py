from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

try:
    from sh import bamToBed
except ImportError:
    raise ImportError('cannot import bamToBed, ensure bedtools are installed')