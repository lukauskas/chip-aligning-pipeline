from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import ftplib
import os
import hashlib
import requests
import tempfile
import shutil

from six.moves.urllib.parse import urlparse

def _fetch_from_ftp(url, output):
    parsed_url = urlparse(url)
    ftp = ftplib.FTP(parsed_url.netloc)

    try:
        ftp.login()
        ftp.cwd(os.path.dirname(parsed_url.path))
        ftp.retrbinary('RETR {0}'.format(os.path.basename(parsed_url.path)), output.write)
    finally:
        ftp.quit()

def _fetch_from_http(url, output):
    response = requests.get(url=url, stream=True)
    response.raise_for_status()

    # Iterate through content 50 MB at a time
    for chunk in response.iter_content(chunk_size=50*1024*1024):
        if chunk:
            output.write(chunk)


def md5_hash(buffer, blocksize=2**20):
    # Based on http://stackoverflow.com/a/4213255
    hash = hashlib.md5()
    for chunk in iter(lambda: buffer.read(blocksize), b''):
        hash.update(chunk)
    return hash.hexdigest()

class ChecksumMismatch(Exception):
    pass

def fetch(url, output, md5_checksum=None):
    parsed_url = urlparse(url)

    if parsed_url.scheme in ['http', 'https']:
        _fetch_from_http(url, output)
    elif parsed_url.scheme == 'ftp':
        _fetch_from_ftp(url, output)
    else:
        raise ValueError("unsupported url scheme {0!r}".format(parsed_url.scheme))

    if md5_checksum is not None:
        output.seek(0)
        actual_checksum = md5_hash(output)

        if actual_checksum != md5_checksum:
            raise ChecksumMismatch('Checksums don\'t match for {}. Got {} != {} (expected)'.format(url, actual_checksum,
                                                                                                   md5_checksum))
