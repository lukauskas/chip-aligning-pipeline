from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import inspect
import logging
import re
import os
import itertools

import luigi
import luigi.format

from chipalign.core.file_formats.file import File, GzippedFile

from luigi.task import flatten

from chipalign.core.util import temporary_directory, ensure_directory_exists_for_file, output_dir, \
    file_modification_time


def _file_safe_string(value):
    value = unicode(value)
    value = re.sub('[^a-zA-Z0-9]', '_', value)
    value = re.sub('__+', '_', value)
    return value.strip('_')

def _collapse_parameters(luigi_params, param_kwargs):
    ans = []
    for param_name, param in luigi_params:
        if param.significant:
            value = param_kwargs[param_name]
            # Flatten list parameters
            if isinstance(value, list) or isinstance(value, tuple):
                param_values = value
            else:
                param_values = [value]

            for param_value in param_values:
                if isinstance(param_value, Task) or isinstance(param_value, MetaTask):
                    # If we got a Task object as a parameter

                    # Add the friendly name of the task to our parameters
                    ans.append(param_value.task_class_friendly_name)
                    # Add the parameters of the task to our parameters
                    ans.extend(param_value.parameters)
                else:
                    # Else just add parameter
                    ans.append(param_value)
    return ans


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
        ans = _collapse_parameters(luigi_params, param_kwargs)

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

        return logging.LoggerAdapter(logger, extra)

    def _flattened_outputs(self):
        return flatten(self.output())

    def _all_outputs_exist(self):
        """
        Returns whether files for each of the outputs exist
        :return:
        """
        outputs = self._flattened_outputs()
        return all(itertools.imap(lambda output: output.exists(), outputs))

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

            mod_dates = itertools.imap(lambda output: output.modification_time, dependency_outputs)
            dependancy_max_mod_date = max(mod_dates)

            if max_dependency_mod_date is None or dependancy_max_mod_date > max_dependency_mod_date:
                max_dependency_mod_date = dependancy_max_mod_date

        outputs = self._flattened_outputs()
        try:
            min_output_mod_date_date = min(itertools.imap(lambda output: output.modification_time, outputs))
        except AttributeError as e:
            raise AttributeError(
                'Incompatible output format for {}. Got {!r}'.format(self.__class__.__name__, e))

        # Ensure all dependencies were built before the parent.
        return max_dependency_mod_date < min_output_mod_date_date

    def _source_code_for_task_has_not_been_modified_since_output_was_generated(self):
        """
        Checks that all outputs have their modification dates greater than or equal to the sourcecode modification time
        """
        outputs = self._flattened_outputs()
        source_modification_time = self._last_modification_for_source()
        for output in outputs:
            if output.modification_time < source_modification_time:
                self.logger().info('Output {} is invalidated as source has been modified since'.format(output))
                return False
        return True

    def complete(self):
        """
            If the task has outputs, check that they all are complete,
            and check that their modification times are all higher than inputs
        """
        outputs = flatten(self.output())
        if len(outputs) == 0:
            return False

        return self._all_outputs_exist() \
               and self._source_code_for_task_has_not_been_modified_since_output_was_generated() \
               and self._dependancies_complete_and_have_lower_modification_dates_than_outputs()

    def temporary_directory(self, **kwargs):
        prefix = kwargs.pop('prefix', 'tmp-{}'.format(self.__class__.__name__))
        return temporary_directory(logger=self.logger(),
                                   prefix=prefix,
                                   **kwargs)

    def ensure_output_directory_exists(self):
        ensure_directory_exists_for_file(os.path.abspath(self.output().path))

    @classmethod
    def _implementation_file(cls):
        return inspect.getsourcefile(cls)

    @classmethod
    def _last_modification_for_source(cls):
        return file_modification_time(cls._implementation_file())

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
        ans = _collapse_parameters(luigi_params, param_kwargs)

        return ans
