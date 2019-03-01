from argparse import REMAINDER

from argcmdr import LocalRoot, local, localmethod


class Management(LocalRoot):
    """management commands for ohio development"""

    package_name = 'ohio'

    @local('remainder', metavar='additional arguments', nargs=REMAINDER)
    def test(self, args):
        """test the codebase"""
        return (self.local.FG, self.local['tox'][args.remainder])

    @local('part', choices=('major', 'minor', 'patch'),
           help="part of the version to be bumped")
    def bump(self, args):
        """increment package version"""
        return self.local['bumpversion'][args.part]

    @local
    def build(self):
        """build the python distribution"""
        return (self.local.FG, self.local['python'][
            'setup.py',
            'sdist',
            'bdist_wheel',
        ])

    @localmethod('versions', metavar='version', nargs='*',
                 help="specific version(s) to upload (default: all)")
    def release(self, args):
        """upload distribution(s) to pypi"""
        if args.versions:
            targets = [f'dist/{self.package_name}-{version}*'
                       for version in args.versions]
        else:
            targets = [f'dist/{self.package_name}-*']
        return (self.local.FG, self.local['twine']['upload'][targets])
