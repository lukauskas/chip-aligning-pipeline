from contextlib import contextmanager

import luigi
from chipalign.core.task import Task
from tests.helpers.task_test import TaskTestCase
from io import StringIO
import sys

class SpecialException(Exception):
    pass

class ExpectedFailureException(Exception):
    pass

class FailingTask(Task):

    x = luigi.Parameter()

    @property
    def _extension(self):
        return 'txt'

    def _run(self):

        if self.x == 0:
            raise SpecialException('X == 0')

        with self.output().open('w') as f:
            f.write(str(self.x))

class FailingTaskLocal(FailingTask):
    run_locally = True

#https://stackoverflow.com/a/17981937
@contextmanager
def captured_output():
    new_out, new_err = StringIO(), StringIO()
    old_out, old_err = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = new_out, new_err
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout, sys.stderr = old_out, old_err

class TestTaskFailuresAreWorkingOnSge(TaskTestCase):

    def test_task_gets_completed(self):
        x = 1
        task = FailingTask(x=x)
        self.build_task(task, expect_failure=False)

        with task.output().open('r') as f:
            x_actual = int(f.read().strip())

        self.assertEqual(x, x_actual)

    def test_task_gets_completed_locally(self):
        x = 1
        task = FailingTaskLocal(x=x)

        self.build_task(task, expect_failure=False)

        with task.output().open('r') as f:
            x_actual = int(f.read().strip())

        self.assertEqual(x, x_actual)

    def test_task_fails(self):
        x = 0
        task = FailingTask(x=x)
        self.build_task(task, expect_failure=True)

    def test_task_fails_locally(self):
        x = 0
        task = FailingTaskLocal(x=x)
        self.build_task(task, expect_failure=True)

