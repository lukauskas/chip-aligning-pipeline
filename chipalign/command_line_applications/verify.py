"""
Module to quickly verify that all command line dependancies are installed.

"""
from io import StringIO

import sh


def _get_shell_output(command,
                      *args, **kwargs):
    ignore_error_return = kwargs.pop('ignore_error_return', False)
    lines = kwargs.pop('lines', 'all')

    with StringIO() as buf:
        try:
            command(*args, **kwargs, _out=buf, _err=buf)
        except sh.ErrorReturnCode:
            if not ignore_error_return:
                raise

        ans = buf.getvalue()

    if lines == 'all':
        return ans
    else:
        ans = ans.split('\n')

        if isinstance(lines, slice):
            ans = '\n'.join(ans[lines])
        else:
            ans = ans[lines]

        return ans

def main():

    print('* Archiving')
    print('** unzip')
    from chipalign.command_line_applications.archiving import unzip
    print(_get_shell_output(unzip, '-v', lines=0))

    print('** gzip')
    from chipalign.command_line_applications.archiving import gzip
    print(_get_shell_output(gzip, '-V', lines=0))

    print('** 7z')
    from chipalign.command_line_applications.archiving import seven_z
    print(_get_shell_output(seven_z, '--help', lines=slice(1, 3)))

    print('* Alignment')
    from chipalign.command_line_applications.bowtie import bowtie2, bowtie2_build
    print(_get_shell_output(bowtie2, '--version', lines=0, ignore_error_return=True))
    print(_get_shell_output(bowtie2_build, '--version', lines=0, ignore_error_return=True))

    print('* Common')
    from chipalign.command_line_applications.common import sort, cat, cut
    print(_get_shell_output(sort, '--version', lines=0))
    print(_get_shell_output(cut, '--version', lines=0))
    print(_get_shell_output(cat, '--version', lines=0))

    print('* Py2 based apps')
    from chipalign.command_line_applications.crossmap import crossmap
    print(_get_shell_output(crossmap, '--help', lines=0))
    from chipalign.command_line_applications.macs import macs2
    print(_get_shell_output(macs2, '--version', lines=0))

    print('* Phantompeakqualtools')
    from chipalign.command_line_applications.phantompeakqualtools import run_spp
    print(_get_shell_output(run_spp, ignore_error_return=True))

    print('* Samtools')
    from chipalign.command_line_applications.samtools import samtools
    print(_get_shell_output(samtools, '--version', lines=0))

    print('* SRA toolkit')
    from chipalign.command_line_applications.sratoolkit import fastq_dump
    print(_get_shell_output(fastq_dump, '--version', lines=1))

    print('* HDF5')
    from chipalign.command_line_applications.tables import h5repack
    print(_get_shell_output(h5repack, '--version', lines=0))

    print('* UCSC Kent Tools')
    from chipalign.command_line_applications.ucsc_suite import twoBitToFa, bigWigToBedGraph, bedClip
    print(_get_shell_output(twoBitToFa, lines=0))
    print(_get_shell_output(bigWigToBedGraph, lines=0))
    print(_get_shell_output(bedClip, lines=0))


if __name__ == '__main__':
    main()