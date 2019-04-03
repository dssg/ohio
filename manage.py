from argparse import REMAINDER

from argcmdr import LocalRoot, local, localmethod


class Management(LocalRoot):
    """management commands for ohio development"""

    package_name = 'ohio'

    bump_default_message = "Bump version: {current_version} → {new_version}"

    @local('remainder', metavar='additional arguments', nargs=REMAINDER)
    def test(self, args):
        """test the codebase"""
        return (self.local.FG, self.local['tox'][args.remainder])

    @local('remainder', metavar='...', nargs=REMAINDER,
           help='for help with underlying command: "manage profile - -h"')
    def profile(self, args):
        """profile the codebase"""
        # -flags intended for subprocess must be distinguished by
        # preceding empty flag / argument "-" or "--"
        # (Or else argparse will copmlain.)
        # But, we don't want to send these on to the subcommand.
        if args.remainder and args.remainder[0] in '--':
            remainder = args.remainder[1:]
        else:
            remainder = args.remainder[:]

        try:
            yield (self.local.FG, self.local['python']['-m', 'prof'][remainder])
        except self.local.ProcessExecutionError as exc:
            raise SystemExit(exc.retcode)

    @localmethod('part', choices=('major', 'minor', 'patch'),
                 help="part of the version to be bumped")
    @localmethod('-t', '--tag-message',
                 help=f"Tag message (in addition to default: "
                      f"'{bump_default_message}')")
    def bump(self, args):
        """increment package version"""
        if args.tag_message:
            tag_message = f"{self.bump_default_message}\n\n{args.tag_message}"
        else:
            tag_message = self.bump_default_message

        return self.local['bumpversion'][
            '--tag-message', tag_message,
            args.part,
        ]

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
