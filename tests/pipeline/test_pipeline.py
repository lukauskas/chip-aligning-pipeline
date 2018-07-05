from tests.helpers.task_test import TaskTestCase

class TestPipeline(TaskTestCase):
    _multiprocess_can_split_ = False

    def test_whole_pipeline(self):
        # Use drosophila datasets as they're smaller ;)

        input_datasets = [
            ('encode', 'ENCFF220GHK'),
            ('encode', 'ENCFF725JVW'),
        ]

        # H3 chip or smth https://www.ncbi.nlm.nih.gov/sra/SRX1619254[accn]
        signal_datasets = [
            ('sra', 'SRR3211630')
        ]

