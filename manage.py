from argparse import REMAINDER

from argcmdr import LocalRoot, local


class Management(LocalRoot):
    """management commands for ohio development"""

    @local('remainder', metavar='additional arguments', nargs=REMAINDER)
    def test(self, args):
        """test the codebase"""
        return (self.local.FG, self.local['tox'][args.remainder])
