from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import sh
from chipalign.command_line_applications.exceptions import log_sh_exceptions

crossmap = log_sh_exceptions(sh.Command("CrossMap.py"))
