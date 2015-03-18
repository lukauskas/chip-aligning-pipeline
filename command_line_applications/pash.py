from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

try:
    from sh import pash3
except ImportError:
    raise ImportError('Cannot import Pash3, ensure pash is installed')