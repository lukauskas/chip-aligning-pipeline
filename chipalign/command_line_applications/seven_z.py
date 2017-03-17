import sh
from chipalign.command_line_applications.exceptions import log_sh_exceptions

seven_z = sh.Command('7z')
seven_z = log_sh_exceptions(seven_z)
