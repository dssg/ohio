import sys
from pathlib import Path
from setuptools import setup

NEEDS_PYTEST = {'pytest', 'test', 'ptr'}.intersection(sys.argv)
PYTEST_RUNNER = ['pytest-runner'] if NEEDS_PYTEST else []

README_PATH = Path(__file__).parent / 'README.md'


setup(
    name='ohio',
    version='0.1.0',
    description="I/O extras",
    long_description=README_PATH.read_text(),
    long_description_content_type='text/markdown',
    author="Center for Data Science and Public Policy",
    author_email='datascifellows@gmail.com',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
    ],
    url="https://github.com/dssg/ohio",
    package_dir={'': 'src'},
    py_modules=['ohio'],
    setup_requires=PYTEST_RUNNER,
    tests_require=['pytest'],
)
