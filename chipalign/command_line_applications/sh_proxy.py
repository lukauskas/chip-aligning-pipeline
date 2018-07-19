# Workaround slow piping
# See https://github.com/amoffat/sh/issues/392#issuecomment-327759713

import sh
import os

_this_dir = os.path.dirname(os.path.abspath(__file__))
_shell_file = os.path.join(_this_dir, 'sh_proxy.sh')

sh_proxy = sh.Command(_shell_file)
