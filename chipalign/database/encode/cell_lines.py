
_ROADMAP_TO_ENCODE_MAP = {
    'E114': 'A549',
    'E115': 'DND-41',
    'E116': 'GM12878',
    'E003': 'H1-hESC',
    'E117': 'HeLa-S3',
    'E118': 'HepG2',
    'E017': 'IMR-90',
    'E123': 'K562',
}

_ENCODE_TO_ROADMAP_MAP = {value: key for key, value in _ROADMAP_TO_ENCODE_MAP.items()}


def roadmap_to_encode(roadmap_cell_line):
    try:
        return _ROADMAP_TO_ENCODE_MAP[roadmap_cell_line]
    except KeyError:
        raise KeyError(
            "Don't know how to convert {!r} to ENCODE cell line".format(roadmap_cell_line))


def encode_to_roadmap(encode_cell_line):
    try:
        return _ENCODE_TO_ROADMAP_MAP[encode_cell_line]
    except KeyError:
        raise KeyError(
            "Don't know how to convert {!r} to ROADMAP cell line".format(encode_cell_line))


