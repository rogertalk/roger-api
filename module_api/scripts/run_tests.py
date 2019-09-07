# -*- coding: utf-8 -*-
#
# Run all the tests by running this in a terminal:
# python -m scripts.run_tests
#
# Run one test by running this:
# python -m scripts.run_tests test_accounts.Messages.test_message_username
#
# Increase verbosity by adding "-v". For more info, read the docs:
# https://docs.python.org/2/library/unittest.html#command-line-interface

import logging
import os.path
import sys
import unittest

import scripts


class RunTests(scripts.ScriptBase):
    def before_setup(self):
        # Add `lib` subdirectory to `sys.path` for third-party libraries.
        lib_path = os.path.join(os.path.dirname(__file__), '../lib')
        sys.path.insert(0, lib_path)

        # Set up code coverage.
        import coverage
        self.cov = coverage.Coverage(include='roger*')
        self.cov.start()

    def main(self):
        # Turn off warnings.
        logging.disable(logging.WARNING)

        # Discover and run unit tests.
        try:
            unittest.main('rogertests')
        except KeyboardInterrupt:
            print 'Interrupted.'
            sys.exit(1)
        except SystemExit as e:
            exit_code = e.code

        # Finalize coverage report.
        self.cov.stop()
        self.cov.save()
        self.cov.html_report()

        # Exit with code indicating success or failure.
        sys.exit(exit_code)


if __name__ == '__main__':
    RunTests().run()
