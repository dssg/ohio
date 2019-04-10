import pathlib
import tempfile

import plumbum
from argparse import REMAINDER

from argcmdr import LocalRoot, local, localmethod


ROOT_DIR = pathlib.Path(__file__).parent.absolute()


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

    @local('target', nargs='?', default='html',
           help="output format for main docs (default: html)")
    def docs(context, args):
        """build documentation & readme"""
        docs_path = ROOT_DIR / 'doc'

        # build "big" docs
        yield (
            context.local.FG,
            context.local['make'][
                '-C', docs_path,
                args.target,
            ]
        )

        with tempfile.TemporaryDirectory() as tmpdirname:
            # build readme
            yield (
                context.local.FG,
                context.local['sphinx-build'][
                    '-a',  # "all": write whether "changed" or not
                    '-b', 'rst',
                    # custom tag to disable links to index page(s)
                    # (it's meant to be a single HTML page after all)
                    '-t', 'noindex',
                    docs_path,
                    tmpdirname,
                ]
            )

            # move readme into place
            # (omitting build artifacts)
            #
            # with `make`, content is put under the subdirectory "singlehtml/";
            # but, we're currently going through `sphinx-build`, which does not.
            #
            # build_path = pathlib.Path(tmpdirname) / 'singlehtml'
            #
            build_path = tmpdirname
            readme_path = ROOT_DIR / 'README.rst'

            with plumbum.local.cwd(build_path):
                # no "_images" nor "_static" for rst
                #
                # yield context.local['cp'][
                #     '-r',
                #     '-t', docs_path,
                #     '_images',
                #     '_static',
                # ]

                yield context.local['cp']['index.rst', readme_path]

        main_docs_path = docs_path / '_build' / args.target

        try:
            main_docs_shortpath = main_docs_path.relative_to(pathlib.Path.cwd())
            readme_shortpath = readme_path.relative_to(pathlib.Path.cwd())
        except ValueError:
            main_docs_shortpath = main_docs_path
            readme_shortpath = readme_path

        print()
        print("docs:", main_docs_shortpath)
        print("readme:", readme_shortpath)

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
