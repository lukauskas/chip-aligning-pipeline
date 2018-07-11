# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
The SunGrid Engine runner

This is a copy of `luigi`'s SGE module, but adapted to work with py3

The main() function of this module will be executed on the
compute node by the submitted job. It accepts as a single
argument the shared temp folder containing the package archive
and pickled task to run, and carries out these steps:
- extract tarfile of package dependencies and place on the path
- unpickle SGETask instance created on the master node
- run SGETask.work()
On completion, SGETask on the master node will detect that
the job has left the queue, delete the temporary folder, and
return from SGETask.run()
"""
import logging
import logging.config
import os
import sys

import luigi

from tblib import pickling_support
pickling_support.install()

try:
    import cPickle as pickle
except ImportError:
    import pickle
import tarfile
import sys

def _do_work_on_compute_node(work_dir, tarball=True):

    if tarball:
        # Extract the necessary dependencies
        # This can create a lot of I/O overhead when running many SGEJobTasks,
        # so is optional if the luigi project is accessible from the cluster node
        _extract_packages_archive(work_dir)

    # Open up the pickle file with the work to be done
    os.chdir(work_dir)
    with open("job-instance.pickle", "rb") as f:
        job = pickle.load(f)

    # Do the work contained
    job.work()


def _extract_packages_archive(work_dir):
    package_file = os.path.join(work_dir, "packages.tar")
    if not os.path.exists(package_file):
        return

    curdir = os.path.abspath(os.curdir)

    os.chdir(work_dir)
    tar = tarfile.open(package_file)
    for tarinfo in tar:
        tar.extract(tarinfo)
    tar.close()
    if '' not in sys.path:
        sys.path.insert(0, '')

    os.chdir(curdir)


def main(args=sys.argv):
    """Run the work() method from the class instance in the file "job-instance.pickle".
    """

    # Set up logging, this is based on
    # https://github.com/spotify/luigi/blob/68fa7bc467ae9cb142206e202a105252bddb6517/luigi/cmdline.py#L44
    config = luigi.configuration.get_config()
    logging_conf = None
    if not config.getboolean('core', 'no_configure_logging', False):
        logging_conf = config.get('core', 'logging_conf_file', None)
        if logging_conf is not None and not os.path.exists(logging_conf):
            raise Exception("Error: Unable to locate specified logging configuration file!")
    if logging_conf is not None:
        logging.config.fileConfig(logging_conf)
    else:
        logging.basicConfig(level=logging.INFO, format=luigi.process.get_log_format())

    logger = logging.getLogger('chipalign.core.sge.sge_runner')
    logger.debug('Running sge_runner from python v{}'.format(sys.version))

    try:
        tarball = "--no-tarball" not in args
        work_dir = args[1]
        logger.debug(f'sge_runner work dir {work_dir}')
        assert os.path.isdir(work_dir), f"Work dir {work_dir!r} does not exist"
        project_dir = args[2]
        sys.path.append(project_dir)
        _do_work_on_compute_node(work_dir, tarball)
    except:
        exc_info = sys.exc_info()
        logging.error("Exception during task execution", exc_info=exc_info)
        # Dump pickled exception
        pickled_ = pickle.dumps(exc_info, protocol=pickle.HIGHEST_PROTOCOL)
        print('__exception_info__start__')
        print(pickled_)
        print('__exception_info__end__')
        raise
    else:
        print('__sge_runner__success__')


if __name__ == '__main__':
    main()