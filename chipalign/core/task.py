from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import logging
import re
import os
import itertools

import luigi
import luigi.format

from chipalign.core.file_formats.file import File, GzippedFile

from luigi.task import flatten

from chipalign.core.util import temporary_directory, ensure_directory_exists_for_file, output_dir


def _file_safe_string(value):
    value = unicode(value)
    value = re.sub('[^a-zA-Z0-9]', '_', value)
    value = re.sub('__+', '_', value)
    return value.strip('_')

class Task(luigi.Task):

    _MAX_LENGTH_FOR_FILENAME = 255 - len('-luigi-tmp-10000000000')

    def __init__(self, *args, **kwargs):
        super(Task, self).__init__(*args, **kwargs)

        # Try generating the filename so exception is raised early, if it is raised
        __ = self._output_filename

    @property
    def task_class_friendly_name(self):
        return self.__class__.__name__

    @property
    def parameters(self):

        luigi_params = self.get_params()
        param_kwargs = self.param_kwargs

        # Create the parameters array from significant parameters list
        ans = []
        for param_name, param in luigi_params:
            if param.significant:
                param_value = param_kwargs[param_name]

                if isinstance(param_value, Task):
                    # If we got a Task object as a parameter

                    # Add the friendly name of the task to our parameters
                    ans.append(param_value.task_class_friendly_name)
                    # Add the parameters of the task to our parameters
                    ans.extend(param_value.parameters)
                else:
                    # Else just add parameter
                    ans.append(param_value)

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
            raise ValueError('Filename {!r} too long. '
                             'Only {!r} characters allowed, consider editing .parameters'.format(filename,
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

        return logging.LoggerAdapter(logger, extra)

    def complete(self):
        """
            If the task has outputs, check that they all are complete,
            and check that their modification times are all higher than inputs
        """
        outputs = flatten(self.output())
        if len(outputs) == 0:
            return False

        dependancies = flatten(self.requires())

        all_outputs_exist = all(itertools.imap(lambda output: output.exists(), outputs))
        if not all_outputs_exist:
            return False
        else:
            if len(dependancies) == 0:
                return True
            else:
                max_dependency_mod_date = None
                for dependency in dependancies:
                    dependency_outputs = flatten(dependency._filename())

                    all_dependencies_satisfied = all(itertools.imap(lambda output: output.exists(), dependency_outputs))
                    if not all_dependencies_satisfied:
                        # If at least one dependency unsatisfied, this task is also not complete
                        return False

                    mod_dates = itertools.imap(lambda output: output.modification_time, dependency_outputs)
                    dependancy_max_mod_date = max(mod_dates)

                    if max_dependency_mod_date is None or dependancy_max_mod_date > max_dependency_mod_date:
                        max_dependency_mod_date = dependancy_max_mod_date

                try:
                    min_output_mod_date_date = min(itertools.imap(lambda output: output.modification_time, outputs))
                except AttributeError as e:
                    raise AttributeError('Incompatible output format for {}. Got {!r}'.format(self.__class__.__name__, e))

                # Ensure all dependencies were built before the parent.
                return max_dependency_mod_date < min_output_mod_date_date

    def temporary_directory(self, **kwargs):
        prefix = kwargs.pop('prefix', 'tmp-{}'.format(self.__class__.__name__))
        cleanup_on_exception = kwargs.pop('cleanup_on_exception', False)
        return temporary_directory(logger=self.logger(),
                                   prefix=prefix, cleanup_on_exception=cleanup_on_exception, **kwargs)

    def ensure_output_directory_exists(self):
        ensure_directory_exists_for_file(os.path.abspath(self.output().path))

class MetaTask(luigi.Task):

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
            return map(lambda x: x._filename(), requires)
        else:
            return requires._filename()

    def requires(self):
        raise NotImplementedError

    def run(self):
        raise Exception('MetaTasks should never be run as they are completed when requires task is complete')

    @property
    def self_parameters(self):
        return []

    @property
    def parameters(self):
        return self.self_parameters + self.requires().parameters