from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import luigi
import yaml


class YamlFile(luigi.File):

    def dump(self, data, safe=True):
        yaml_dump = yaml.safe_dump if safe else yaml.dump
        with self.open('w') as f:
            yaml_dump(data, f)

    def load(self, safe=True):
        yaml_load = yaml.safe_load if safe else yaml.load
        with self.open('r') as f:
            return yaml_load(f)
