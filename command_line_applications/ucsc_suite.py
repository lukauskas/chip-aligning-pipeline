from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

try:
    from sh import twoBitToFa
except ImportError:
    raise ImportError('Cannot import twoBitToFa. Ensure ucsc kent genome tools suite is installed in your system')

try:
    from sh import bigWigToBedGraph
except ImportError:
    raise ImportError('Cannot import bigWigToBedGraph. Ensure ucsc kent genome tools suite is installed in your system')