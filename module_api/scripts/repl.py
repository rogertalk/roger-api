# -*- coding: utf-8 -*-

import code
import os.path
import sys

import scripts


class REPL(scripts.ScriptBase):
    def before_setup(self):
        # Add `lib` subdirectory to `sys.path` for third-party libraries.
        lib_path = os.path.join(os.path.dirname(__file__), '../lib')
        sys.path.insert(0, lib_path)

    def main(self):
        # Start the REPL.
        code.interact()


if __name__ == '__main__':
    REPL().run()
