try:
    from sh import fseq
except ImportError:
    raise ImportError('Cannot import fseq from your system, ensure it is installed')