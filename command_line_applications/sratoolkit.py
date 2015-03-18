from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

try:
    from sh import fastq_dump
except ImportError:
    raise ImportError('Cannot import fastq-dump. Ensure SRA toolkit is installed')