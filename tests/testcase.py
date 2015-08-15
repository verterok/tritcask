#
# Author: Guillermo Gonzalez <guillermo.gonzalez@canonical.com>
#
# Copyright 2010-2012 Canonical Ltd.
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 3, as published
# by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranties of
# MERCHANTABILITY, SATISFACTORY QUALITY, or FITNESS FOR A PARTICULAR
# PURPOSE.  See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# In addition, as a special exception, the copyright holders give
# permission to link the code of portions of this program with the
# OpenSSL library under certain conditions as described in each
# individual source file, and distribute linked combinations
# including the two.
# You must obey the GNU General Public License in all respects
# for all of the code used other than OpenSSL.  If you modify
# file(s) with this exception, you may extend this exception to your
# version of the file(s), but you are not obligated to do so.  If you
# do not wish to do so, delete this exception statement from your
# version.  If you delete this exception statement from all source
# files in the program, then also delete it here.
"""Base tests cases and test utilities."""

import logging
import os
import shutil
import sys
import tempfile
import uuid

from testtools import TestCase
from contrib.handlers import MementoHandler
from tritcask import logger, DataFile


class BaseTestCase(TestCase):
    """Base TestCase with helper methods for tritcask tests.

    This class provides:
        mktemp(name): helper to create temporary dirs
        rmtree(path): support read-only shares
    """
    def setUp(self):
        super(BaseTestCase, self).setUp()
        self.__root = None
        self._base_temp_dir = None
        self.base_dir = self.mktemp('data_dir')
        self.root_dir = self.mktemp('root_dir')
        self.memento = MementoHandler()
        logger.addHandler(self.memento)
        self.addCleanup(logger.removeHandler, self.memento)
        logger.setLevel(logging.DEBUG)
        # reset this class attribute after each test, otherwise
        # it stays dirty after some tests
        self.addCleanup(setattr, DataFile, 'last_generated_id', 0)

    def tearDown(self):
        """Cleanup the temp dir."""
        # invalidate the current config
        if self.__root is not None and os.path.exists(self.__root):
            self.rmtree(self.__root)
        if self._base_temp_dir is not None and \
                os.path.exists(self._base_temp_dir):
            self.rmtree(self._base_temp_dir)
        super(BaseTestCase, self).tearDown()

    def mktemp(self, name=None):
        """Customized mktemp that accepts an optional name argument."""
        tempdir = os.path.join(self.tmpdir, name)
        if os.path.exists(tempdir):
            self.rmtree(tempdir)
        os.makedirs(tempdir)
        return tempdir

    @property
    def tmpdir(self):
        """Default tmpdir: module/class/test_method."""
        # check if we already generated the root path
        if self.__root is not None and os.path.exists(self.__root):
            return self.__root

        self._base_temp_dir = tempfile.mkdtemp(suffix='tritcask_tests')
        MAX_FILENAME = 32  # some platforms limit lengths of filenames
        base = os.path.join(self.__class__.__module__[:MAX_FILENAME],
                            self.__class__.__name__[:MAX_FILENAME],
                            self._testMethodName[:MAX_FILENAME])
        # use _trial_temp dir, it should be os.gwtcwd()
        # define the root temp dir of the testcase, pylint: disable-msg=W0201
        self.__root = os.path.join(self._base_temp_dir, base)
        return self.__root

    def rmtree(self, path):
        """Custom rmtree that handle ro parent(s) and childs."""
        # on windows the paths cannot be removed because the process running
        # them has the ownership and therefore are locked.
        if sys.platform == 'win32':
            return
        if not os.path.exists(path):
            return
        shutil.rmtree(path)

    def build_data(self):
        """Build a random key/value."""
        key = str(uuid.uuid4()).encode('ascii')
        data = os.urandom(50)
        return key, data
