from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import ftplib
import os
import requests
import tempfile
import shutil

from urlparse import urlparse

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

    # Iterate through content 10 MB at a time
    for chunk in response.iter_content(chunk_size=10*1024*1024):
        if chunk:
            output.write(chunk)


def fetch(url, output):
    parsed_url = urlparse(url)

    if parsed_url.scheme in ['http', 'https']:
        _fetch_from_http(url, output)
    elif parsed_url.scheme == 'ftp':
        _fetch_from_ftp(url, output)
    else:
        raise ValueError("unsupported url scheme {0!r}".format(parsed_url.scheme))