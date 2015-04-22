from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

try:
    from sh import samtools
except ImportError:
    samtools = None  # To shut PyCharm's code interpreter up
    raise ImportError("Cannot import samtools from your path, ensure samtools package installed")
