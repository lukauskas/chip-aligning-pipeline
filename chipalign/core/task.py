from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import collections
import shutil
from builtins import str

import sys
import time

from six import reraise

try:
    import cPickle as pickle
except ImportError:
    import pickle


import hashlib
import inspect
import logging
import re
import os
import subprocess

import luigi.format
from chipalign.core.sge import sge_runner

from chipalign.core.file_formats.file import File, GzippedFile

from luigi.task import flatten

from chipalign.core.logging import LoggerWithExtras
from chipalign.core.util import temporary_directory, ensure_directory_exists_for_file, output_dir, \
    file_modification_time, timed_segment, temp_dir, use_sge, sge_no_tarball, sge_parallel_env

from luigi.contrib.sge import SGEJobTask, _parse_qsub_job_id, \
    _parse_qstat_state

def _build_qsub_command(cmd, job_name, outfile, errfile, pe, n_cpu):
    """Submit shell command to SGE queue via `qsub`"""
    qsub_template = """echo {cmd} | qsub -cwd -o ":{outfile}" -e ":{errfile}" -S /bin/bash -V -r y -pe {pe} {n_cpu} -N {job_name}"""
    return qsub_template.format(
        cmd=cmd, job_name=job_name, outfile=outfile, errfile=errfile,
        pe=pe, n_cpu=n_cpu)


def _file_safe_string(value):
    value = str(value)
    value = re.sub('[^a-zA-Z0-9]', '_', value)
    value = re.sub('__+', '_', value)
    return value.strip('_')

def _collapse_parameters(luigi_params, param_kwargs, hash_params=None):
    ans = []

    if hash_params is None:
        hash_params = set()

    for param_name, param in luigi_params:
        if param.significant:
            value = param_kwargs[param_name]
            # Flatten list parameters
            if isinstance(value, list) or isinstance(value, tuple):
                param_values = value
            else:
                param_values = [value]

            param_ans = []
            for param_value in param_values:
                if isinstance(param_value, Task) or isinstance(param_value, MetaTask):
                    # If we got a Task object as a parameter

                    # Add the friendly name of the task to our parameters
                    param_ans.append(param_value.task_class_friendly_name)
                    # Add the parameters of the task to our parameters
                    param_ans.extend(param_value.parameters)
                else:
                    # Else just add parameter
                    param_ans.append(param_value)

            if param_name in hash_params:
                ans.append(hashlib.sha1(str(param_ans).encode('utf-8')).hexdigest()[:8])
            else:
                ans.extend(param_ans)
    return ans


class Task(SGEJobTask):
    _MAX_LENGTH_FOR_FILENAME = 255 - len('-luigi-tmp-10000000000')
    _parameter_names_to_hash = None

    n_cpu = luigi.IntParameter(default=1, significant=False)
    shared_tmp_dir = luigi.Parameter(default=temp_dir(), significant=False)
    parallel_env = luigi.Parameter(default=sge_parallel_env(), significant=False)
    run_locally = luigi.BoolParameter(
        default=not use_sge(),
        significant=False,
        description="run locally instead of on the cluster")
    no_tarball = luigi.BoolParameter(
        default=sge_no_tarball(),
        significant=False,
        description="don't tarball (and extract) the luigi project files")

    def __init__(self, *args, **kwargs):
        super(Task, self).__init__(*args, **kwargs)

        # Try generating the filename so exception is raised early, if it is raised
        __ = self._output_filename

    # This is a copy-paste of the parent class, but with some py3 things fixed
    def _dump(self, out_dir=''):
        """Dump instance to file."""
        with self.no_unpicklable_properties():
            self.job_file = os.path.join(out_dir, 'job-instance.pickle')
            if self.__module__ == '__main__':
                d = pickle.dumps(self)
                module_name = os.path.basename(sys.argv[0]).rsplit('.', 1)[0]
                d = d.replace('(c__main__', "(c" + module_name)

                with open(self.job_file, 'wb') as f:
                    f.write(d)
            else:

                with open(self.job_file, 'wb') as f:
                    pickle.dump(self, f, protocol=pickle.HIGHEST_PROTOCOL)

    # More py3 fixes
    def _run_job(self):
        logger = self.logger()
        # Build a qsub argument that will run sge_runner.py on the directory we've specified
        runner_path = sge_runner.__file__
        if runner_path.endswith("pyc"):
            runner_path = runner_path[:-3] + "py"
        job_str = 'python {0} "{1}" "{2}"'.format(
            runner_path, self.tmp_dir,
            os.getcwd())  # enclose tmp_dir in quotes to protect from special escape chars
        if self.no_tarball:
            job_str += ' "--no-tarball"'

        # Build qsub submit command
        self.outfile = os.path.join(self.tmp_dir, 'job.out')
        self.errfile = os.path.join(self.tmp_dir, 'job.err')
        submit_cmd = _build_qsub_command(job_str, self.task_family, self.outfile,
                                         self.errfile, self.parallel_env, self.n_cpu)
        logger.debug('qsub command: \n{}'.format(submit_cmd))

        # Submit the job and grab job ID
        output = subprocess.check_output(submit_cmd, shell=True)
        self.job_id = _parse_qsub_job_id(output)
        logger.debug(f"Submitted job to qsub with response:\n{output}")

        self._track_job()

        # Now delete the temporaries, if they're there.
        if self.tmp_dir and os.path.exists(self.tmp_dir) and not self.dont_remove_tmp_dir:
            logger.info('Removing temporary directory {}'.format(self.tmp_dir))
            shutil.rmtree(self.tmp_dir)

    def _reraise_task_failures(self):
        logger = self.logger()
        if not os.path.exists(self.errfile):
            logger.error('Error fetching task error output')
            raise Exception('Task failed, additionally fetching error output failed too')

        with open(self.errfile, "r") as f:
            error_txt = f.read()

        if '__sge_runner__success__':
            logger.debug('No failures to re-raise, job executed successfully')
        else:
            match = re.match('__exception_info__start__\n(?P<exc_info>.*)\n__exception_info__end__', error_txt)
            if match is None:
                raise Exception('Task failed, but no exception could be parsed. Raw error output\n{}'.format(error_txt))
            else:
                reraise(*pickle.loads(*match.group('exc_info')))


    # I swear I am rewriting everything here.
    def _track_job(self):
        logger = self.logger()
        while True:
            # Sleep for a little bit

            time.sleep(self.poll_time)

            # See what the job's up to
            # ASSUMPTION
            qstat_out = subprocess.check_output(['qstat'])
            sge_status = _parse_qstat_state(qstat_out.decode('utf-8'), self.job_id)
            if sge_status == 'r':
                # running
                # logger.info('Job is running...')
                continue
            elif sge_status == 'qw':
                # logger.info('Job is pending...')
                continue
            elif 'E' in sge_status:
                logger.error(f'qsub job failed: status={sge_status}')
                self._reraise_task_failures()
                break
            elif sge_status == 't' or sge_status == 'u':
                logger.info(f'qsub job status: status={sge_status} (could indicate either success of failure)')
                self._reraise_task_failures()
                break
            else:
                logger.error('qsub job status unknown: {}'.format(sge_status))
                raise Exception(
                    f"job status isn't one of ['r', 'qw', 'E*', 't', 'u']: {sge_status}")

    def work(self):
        self.ensure_output_directory_exists()
        logger = self.logger()

        # Time the run for statistics
        with timed_segment(self.__class__.__name__,
                           timed_segment_type='task',
                           logger=logger):
            return self._run()

    def run(self):
        if self.run_locally:
            return self.work()
        else:
            self._init_local()
            self._run_job()
            # The procedure:
            # - Pickle the class
            # - Tarball the dependencies
            # - Construct a qsub argument that runs a generic runner function with the path to the pickled class
            # - Runner function loads the class from pickle
            # - Runner class untars the dependencies
            # - Runner function hits the button on the class's work() method

    def _run(self):
        """
        Override this method, rather than .run() (as the former does some heavy lifting)
        :return:
        """
        raise NotImplementedError

    @property
    def task_class_friendly_name(self):
        return self.__class__.__name__

    @property
    def parameters(self):

        luigi_params = self.get_params()
        param_kwargs = self.param_kwargs

        # Create the parameters array from significant parameters list
        ans = _collapse_parameters(luigi_params, param_kwargs,
                                   hash_params=self._parameter_names_to_hash)

        return ans

    @property
    def _basename(self):
        parameters = self.parameters

        basename_components = parameters
        basename_components = map(_file_safe_string, basename_components)
        basename = u'.'.join(basename_components)
        return basename

    @property
    def _extension(self):
        raise NotImplementedError

    @property
    def _output_filename(self):
        filename = u'.'.join([self._basename, self._extension])

        if len(filename) > self._MAX_LENGTH_FOR_FILENAME:
            raise ValueError('Filename for {} too long: {!r} '
                             'Only {!r} characters allowed, consider editing .parameters'.format(
                self.__class__.__name__,
                filename,
                self._MAX_LENGTH_FOR_FILENAME))

        return filename

    def _output_directory(self):
        return os.path.join(output_dir(), self.__class__.__name__)

    @property
    def __full_path(self):
        return os.path.join(self._output_directory(),
                            self._output_filename)

    @property
    def _output_class(self):
        if self._extension.endswith('.gz'):
            return GzippedFile
        else:
            return File

    def output(self):
        path = self.__full_path
        return self._output_class(path)

    @classmethod
    def class_logger(cls):
        logger = logging.getLogger('task.{}'.format(cls.__name__))
        return logger

    def logger(self):
        logger = self.class_logger()

        extra = {'class': self.__class__.__name__,
                 'parameters': '.'.join(map(str, self.parameters)),
                 'output_filename': self._output_filename}

        return LoggerWithExtras(logger, extra)

    def _flattened_outputs(self):
        return flatten(self.output())

    def _all_outputs_exist(self):
        """
        Returns whether files for each of the outputs exist
        :return:
        """
        outputs = self._flattened_outputs()
        return all(map(lambda output: output.exists(), outputs))

    def _dependancies_complete_and_have_lower_modification_dates_than_outputs(self):
        """
        Returns true if the dependancy modification dates are lower than the modification date of current task
        :return:
        """
        dependancies = flatten(self.requires())
        if len(dependancies) == 0:
            # No dependancies -- we're good
            return True

        max_dependency_mod_date = None
        for dependency in dependancies:
            dependency_outputs = flatten(dependency.output())

            if not dependency.complete():
                self.logger().debug('{} is not complete as {} is not complete'.format(self.__class__.__name__,
                                                                                      dependency.__class__.__name__))
                return False

            mod_dates = map(lambda output: output.modification_time, dependency_outputs)
            dependancy_max_mod_date = max(mod_dates)

            if max_dependency_mod_date is None or dependancy_max_mod_date > max_dependency_mod_date:
                max_dependency_mod_date = dependancy_max_mod_date

        outputs = self._flattened_outputs()

        try:
            min_output_mod_date = min(map(lambda output: output.modification_time, outputs))
        except AttributeError as e:
            raise AttributeError(
                'Incompatible output format for {}. Got {!r}'.format(self.__class__.__name__, e))

        # Ensure all dependencies were built before the parent.
        try:
            return max_dependency_mod_date <= min_output_mod_date
        except TypeError:
            if max_dependency_mod_date is None:
                return False
            else:
                raise

    def _source_code_for_task_has_not_been_modified_since_output_was_generated(self):
        """
        Checks that all outputs have their modification dates greater than or equal to the sourcecode modification time
        """
        outputs = self._flattened_outputs()
        source_modification_time = self._last_modification_for_source()
        for output in outputs:
            if not output.modification_time or output.modification_time < source_modification_time:
                self.logger().info('Output {} is invalidated as source has been modified since'.format(output))
                return False
        return True

    def complete(self):
        """
            If the task has outputs, check that they all are complete,
            and check that their modification times are all higher than inputs
        """
        logger = self.logger()

        outputs = flatten(self.output())
        if len(outputs) == 0:
            return False

        try:
            completion = self._all_outputs_exist() \
                         and self._source_code_for_task_has_not_been_modified_since_output_was_generated() \
                         and self._dependancies_complete_and_have_lower_modification_dates_than_outputs()
        except Exception:
            logger.error('Exception when checking for completeness of {}'.format(self.__class__.__name__))
            raise

        return completion

    def temporary_directory(self, **kwargs):
        prefix = kwargs.pop('prefix', 'tmp-{}'.format(self.__class__.__name__))
        return temporary_directory(logger=self.logger(),
                                   prefix=prefix,
                                   **kwargs)

    def ensure_output_directory_exists(self):
        output = self.output()
        if not isinstance(output, collections.Iterable):
            ensure_directory_exists_for_file(os.path.abspath(output.path))
        else:
            for sub_output in output:
                ensure_directory_exists_for_file(os.path.abspath(sub_output.path))

    @classmethod
    def _implementation_file(cls):
        return inspect.getsourcefile(cls)

    @classmethod
    def _last_modification_for_source(cls):
        return file_modification_time(cls._implementation_file())

class MetaTask(luigi.Task):
    _parameter_names_to_hash = None

    @property
    def task_class_friendly_name(self):
        return self.__class__.__name__

    def complete(self):
        requires = self.requires()
        if isinstance(requires, list):
            return all(map(lambda x: x.complete(), requires))
        else:
            return requires.complete()

    def output(self):
        requires = self.requires()
        if isinstance(requires, list):
            return map(lambda x: x.output(), requires)
        else:
            return requires.output()

    def requires(self):
        raise NotImplementedError

    def run(self):
        pass  # Should not do anything

    @property
    def parameters(self):

        luigi_params = self.get_params()
        param_kwargs = self.param_kwargs

        # Create the parameters array from significant parameters list
        ans = _collapse_parameters(luigi_params, param_kwargs, hash_params=self._parameter_names_to_hash)

        return ans
