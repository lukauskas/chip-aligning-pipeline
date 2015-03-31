from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import unittest

from tests.roadmap_compatibility.roadmap_tag import roadmap_test



@roadmap_test
class TestTagFiltering(unittest.TestCase):


    def test_can_reproduce_h3k56ac_dataset_from_initial_reads(self):

