from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import luigi
import unittest
from chipalign.core.task import Task, MetaTask



class A(Task):

    x = luigi.Parameter()
    y = luigi.Parameter(default='y')

    z = luigi.Parameter(significant=False)

    @property
    def _extension(self):
        return 'ext'

class B(Task):
    x = luigi.Parameter()

    @property
    def task_class_friendly_name(self):
        return 'bfriendly'

    @property
    def _extension(self):
        return 'derp'

    _
class Meta(MetaTask):

    derp = luigi.Parameter()

    def requires(self):
        return A(x='x', z='z')


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

    def test_metatasks_are_reproduced_correctly(self):

        b = Meta(derp='derpina')
        a = A(x=b, z='derp')

        expected_parameters = [b.task_class_friendly_name, 'derpina', 'y']
        self.assertListEqual(expected_parameters, a.parameters)

    def test_lists_are_reproduced_as_separate_parameters(self):
        b = B(x='bx')
        c = B(x='bc')
        a = A(x=[b, c], z='derp')

        expected_parameters = [b.task_class_friendly_name, 'bx', c.task_class_friendly_name, 'bc', 'y']
        self.assertListEqual(expected_parameters, a.parameters)

    def test_list_of_tasks_reproduced_faithfully(self):
        a = A(x=['x', 'xx', 'xxx'], z='derp')
        expected_parameters = ['x', 'xx', 'xxx', 'y']
        self.assertListEqual(expected_parameters, a.parameters)


class TestTaskFilenameIsCorrect(unittest.TestCase):

    def test_parameters_are_joined_correctly(self):
        a = A(x='x', y='y', z='z')
        self.assertEqual('x.y', a._basename)

    def test_special_characters_are_escaped_correctly(self):
        a = A(x='5.123', y='y++154-44--!//', z='z')
        self.assertEqual('5_123.y_154_44', a._basename)

    def extension_is_added_correctly(self):
        a = A(x='x', y='y', z='z')
        self.assertEqual('x.y.ext', a._output_filename)

    def test_long_filename_raises_exception(self):
        # FAT32 allows 255 chars for filename, luigi adds the luigi-tmp suffix sometimes so we need to allow for it
        MAX_LENGTH_FOR_TASK_FILENAME = Task._MAX_LENGTH_FOR_FILENAME

        characters_left_for_x = MAX_LENGTH_FOR_TASK_FILENAME - len('.y') - len('.ext')

        # Both should be fine
        try:
            less_than_limit = A(x='x' * (characters_left_for_x-1), y='y', z='not important')
            equal_to_limit = A(x='x' * characters_left_for_x, y='y', z='not important')
        except Exception as e:
            self.fail('Got {!r}, while expected no exception'.format(e))

        # One should not be able to create this class
        self.assertRaises(ValueError, A, x='x' * (characters_left_for_x+1), y='y', z='not important')
