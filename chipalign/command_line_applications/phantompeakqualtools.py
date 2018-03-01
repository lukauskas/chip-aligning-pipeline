import sh
from chipalign.command_line_applications.exceptions import log_sh_exceptions

run_spp = sh.Command('run_spp.R')
run_spp = log_sh_exceptions(run_spp)