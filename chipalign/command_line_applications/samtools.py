from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from chipalign.command_line_applications.exceptions import log_sh_exceptions

try:
    from sh import samtools
    samtools = log_sh_exceptions(samtools)
except ImportError:
    samtools = None  # To shut PyCharm's code interpreter up
    raise ImportError("Cannot import samtools from your path, ensure samtools package installed")
