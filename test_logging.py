import logging
import logging.config
import sys

logging.config.fileConfig('logging.conf')

def main():
    # Based on python-logstash package example

    logger = logging.getLogger('test_logging')

    logger.error('chipalign: test logstash error message.')
    logger.info('chipalign: test logstash info message.')
    logger.warning('chipalign: test logstash warning message.')

    # add extra field to logstash message
    extra = {
        'test_string': 'python version: ' + repr(sys.version_info),
        'test_boolean': True,
        'test_dict': {'a': 1, 'b': 'c'},
        'test_float': 1.23,
        'test_integer': 123,
        'test_list': [1, 2, '3'],
    }
    logger.info('chipalign: test extra fields', extra=extra)


if __name__ == '__main__':
    main()