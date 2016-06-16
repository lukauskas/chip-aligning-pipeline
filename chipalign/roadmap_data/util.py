from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import re

def signal_sortkey(track_name):
    """
    Provides a sortkey for the histone signal tracks in roadmap

    :param track_name:
    :return:
    """
    rank, protein, pos, mod = None, None, None, None
    if track_name == 'DNase':
        rank = 0
    else:
        match = re.match('(?P<histone>H3|H4|H2B|H2A|H2A.Z)((?P<res>\w)(?P<pos>\d+)(?P<mod>\w+))?',
                         track_name)
        if not match:
            raise Exception('Cannot parse {!r}'.format(track_name))

        rank = 1
        protein = match.group('histone')
        pos = int(match.group('pos')) if match.group('pos') is not None else None
        mod = match.group('mod')

    return rank, protein, pos, mod