from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

try:
    from sh import twoBitToFa
except ImportError:
    twoBitToFa = None
    raise ImportError('Cannot import twoBitToFa. Ensure UCSC kent genome tools suite is installed in your system')

try:
    from sh import bigWigToBedGraph
except ImportError:
    bigWigToBedGraph = None
    raise ImportError('Cannot import bigWigToBedGraph. Ensure UCSC kent genome tools suite is installed in your system')

try:
    from sh import bedClip
except ImportError:
    bedClip = None
    raise ImportError('Cannot import bedClip. Ensure UCSC kent genome tools suite is installed in your system')