import re
import sys
from pathlib import Path
from setuptools import find_packages, setup

NEEDS_PYTEST = {'pytest', 'test', 'ptr'}.intersection(sys.argv)
PYTEST_RUNNER = ['pytest-runner'] if NEEDS_PYTEST else []

ROOT_PATH = Path(__file__).parent

README_PATH = ROOT_PATH / 'README.rst'

REQUIREMENTS_TEST_PATH = ROOT_PATH / 'requirement' / 'test.txt'


def stream_requirements(fd):
    """For a given requirements file descriptor, generate lines of
    distribution requirements, ignoring comments and chained requirement
    files.

    """
    for line in fd:
        cleaned = re.sub(r'#.*$', '', line).strip()
        if cleaned and not cleaned.startswith('-r'):
            yield cleaned


with REQUIREMENTS_TEST_PATH.open() as test_requirements_file:
    REQUIREMENTS_TEST = list(stream_requirements(test_requirements_file))


setup(
    name='ohio',
    version='0.3.1',
    description="I/O extras",
    long_description=README_PATH.read_text(),
    long_description_content_type='text/x-rst',
    author="Center for Data Science and Public Policy",
    author_email='datascifellows@gmail.com',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
    ],
    url="https://github.com/dssg/ohio",
    package_dir={'': 'src'},
    packages=find_packages('src'),
    setup_requires=PYTEST_RUNNER,
    tests_require=REQUIREMENTS_TEST,
)
