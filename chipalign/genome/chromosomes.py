import luigi

from chipalign.core.task import Task
from chipalign.core.file_formats.yaml_file import YamlFile
from chipalign.core.util import autocleaning_pybedtools


class Chromosomes(Task):
    """
    Saves chromosome information to file, allows pre-filtering.

    :param genome_version: genome version to use
    :param collection: collection of chromosomes to use.
                       Either 'male', 'female', 'all' or a particular chromosome
                       Defaults to 'all'
    """

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

    def _run(self):

        with autocleaning_pybedtools() as pybedtools:
            chromsizes = pybedtools.chromsizes(self.genome_version)
            chromsizes = dict(chromsizes)

            if not self.genome_version.startswith('hg'):
                raise Exception('Not sure how to parse collections for genome {}'.format(self.genome_version))

            female_chromosomes = {'chr{}'.format(x) for x in (list(range(1, 23)) + ['X', 'M'])}
            male_chromosomes = female_chromosomes | {'chrY'}

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