from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

try:
    from cmd import bowtie2, bowtie2_build
except ImportError:
    raise ImportError('Cannot find bowtie2 or bowtie2-build executables in the user\'s system. '
                      'Please install bowtie2 using your system\'s package manager')