from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
# each can be downloaded from http://hgdownload.soe.ucsc.edu/admin/exe/linux.x86_64/
from chipalign.command_line_applications.exceptions import log_sh_exceptions

try:
    from sh import twoBitToFa
    twoBitToFa = log_sh_exceptions(twoBitToFa)
except ImportError:
    twoBitToFa = None
    raise ImportError('Cannot import twoBitToFa. Ensure UCSC kent genome tools suite is installed in your system')

try:
    from sh import bigWigToBedGraph
    bigWigToBedGraph = log_sh_exceptions(bigWigToBedGraph)
except ImportError:
    bigWigToBedGraph = None
    raise ImportError('Cannot import bigWigToBedGraph. Ensure UCSC kent genome tools suite is installed in your system')

try:
    from sh import bedClip
    bedClip = log_sh_exceptions(bedClip)
except ImportError:
    bedClip = None
    raise ImportError('Cannot import bedClip. Ensure UCSC kent genome tools suite is installed in your system')