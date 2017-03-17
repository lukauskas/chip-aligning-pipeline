from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from chipalign.command_line_applications.exceptions import log_sh_exceptions

try:
    from sh import fastq_dump
    fastq_dump = log_sh_exceptions(fastq_dump)
except ImportError:
    fastq_dump = None  # So the code interpreter is OK
    raise ImportError('Cannot import fastq-dump. Ensure SRA toolkit is installed')