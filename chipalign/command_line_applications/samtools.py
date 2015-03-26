from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

try:
    from sh import samtools
except ImportError:
    raise ImportError("Cannot import samtools from your path, ensure samtools package installed")