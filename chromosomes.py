import pybedtools
import luigi

from task import Task
from file_formats.yaml_file import YamlFile


class Chromosomes(Task):

    genome_version = luigi.Parameter()
    collection = luigi.Parameter('all')  # All, male/female

    @property
    def parameters(self):
        return [self.genome_version, self.collection]

    @property
    def _extension(self):
        return 'list'

    def output(self):
        super_output_path = super(Chromosomes, self).output().path
        return YamlFile(super_output_path)

    def run(self):

        chromsizes = pybedtools.chromsizes(self.genome_version)

        female_chromosomes = {'chr{}'.format(x) for x in (range(1, 23) + ['X'])}
        male_chromosomes = female_chromosomes | {'Y'}

        if self.collection == 'male':
            chromsizes = {k: chromsizes[k] for k in male_chromosomes}
        elif self.collection == 'female':
            chromsizes = {k: chromsizes[k] for k in female_chromosomes}
        elif self.collection == 'all':
            pass
        elif self.collection in chromsizes:
            chromsizes = {self.collection: chromsizes[self.collection]}
        else:
            raise ValueError('Unknown value for collection: {!r}'.format(self.collection))

        self.output().dump(chromsizes)

if __name__ == '__main__':
    luigi.run(main_task_cls=Chromosomes)