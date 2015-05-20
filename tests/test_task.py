from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import luigi
import unittest
from chipalign.core.task import Task


class A(Task):

    x = luigi.Parameter()
    y = luigi.Parameter(default='y')

    z = luigi.Parameter(significant=False)

class B(Task):
    x = luigi.Parameter()

    @property
    def task_class_friendly_name(self):
        return 'bfriendly'

class TestTaskParameterHelpers(unittest.TestCase):

    def test_task_default_parameters_is_set_to_all_significant_params(self):

        a = A(x='d', z='derp')
        expected_parameters = ['d', 'y']
        self.assertListEqual(expected_parameters, a.parameters)

        a = A(x='d', y='blah', z='derp')
        expected_parameters = ['d', 'blah']
        self.assertListEqual(expected_parameters, a.parameters)

    def test_task_reproduces_child_parameters_if_another_task_given(self):

        b = B(x='bx')
        a = A(x=b, z='derp')

        expected_parameters = [b.task_class_friendly_name, 'bx', 'y']
        self.assertListEqual(expected_parameters, a.parameters)