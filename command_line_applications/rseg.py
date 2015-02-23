from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

try:
    from sh import deadzones
except ImportError:
    raise ImportError('Cannot import rseg or one of its utilities, ensure it is correctly installed')