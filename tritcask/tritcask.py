# tritcask - key/value store
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
"""Bitcask-like (row_type,key)/value store."""

from __future__ import division

import contextlib
import logging
import mmap
import os
import struct
import sys
import time
import tempfile
import uuid
import zlib

try:
    import cPickle as pickle
except ImportError:
    # alternate import for py3
    import pickle

try:
    from itertools import ifilter
except ImportError:
    # for py3
    ifilter = filter

from operator import attrgetter
from collections import namedtuple

try:
    from UserDict import DictMixin
except ImportError:
    # for py3
    from collections import MutableMapping as DictMixin

crc32_fmt = '>I'  # from Py3 the crc will be 0->2**32-1
crc32_size = struct.calcsize(crc32_fmt)
crc32_struct = struct.Struct(crc32_fmt)

header_fmt = '>diii'
header_size = struct.calcsize(header_fmt)
header_struct = struct.Struct(header_fmt)

hint_header_fmt = '>diiii'
hint_header_size = struct.calcsize(hint_header_fmt)
hint_header_struct = struct.Struct(hint_header_fmt)

TOMBSTONE = str(uuid.uuid5(uuid.NAMESPACE_OID, 'TOMBSTONE')).encode('ascii')
TOMBSTONE_POS = -1

LIVE = '.live'
INACTIVE = '.inactive'
HINT = '.hint'
DEAD = '.dead'
BROKEN = '.broken'

VERSION = 'v1'
FILE_SUFFIX = '.tritcask-%s.data' % VERSION

EXTRA_SEEK = False
if sys.platform == 'win32':
    EXTRA_SEEK = True

logger = logging.getLogger('tritcask')


class BadCrc(Exception):
    """A Exception for Bad CRC32."""


class BadHeader(Exception):
    """A Exception for Bad header value."""


TritcaskEntry = namedtuple('TritcaskEntry', ['crc32', 'tstamp', 'key_sz',
                           'value_sz', 'row_type', 'key', 'value', 'value_pos'])


_HintEntry = namedtuple('_HintEntry', ['tstamp', 'key_sz', 'row_type',
                                       'value_sz', 'value_pos', 'key'])


class HintEntry(_HintEntry):
    """A entry for the hint file."""

    @classmethod
    def from_tritcask_entry(cls, entry, dead=False):
        """Return a KeydirEntry from a file_id + TritcaskEntry."""
        value_pos = TOMBSTONE_POS if dead else entry.value_pos
        return cls(entry.tstamp, entry.key_sz, entry.row_type,
                   entry.value_sz, value_pos, entry.key)

    @property
    def header(self):
        """Return the header tuple for this entry."""
        return self[:-1]


_KeydirEntry = namedtuple('_KeydirEntry',
                          ['file_id', 'tstamp', 'value_sz', 'value_pos'])


class KeydirEntry(_KeydirEntry):
    """A entry for the Keydir."""

    @classmethod
    def from_tritcask_entry(cls, file_id, entry):
        """Return a KeydirEntry from a file_id + TritcaskEntry."""
        return cls(file_id, entry.tstamp, entry.value_sz, entry.value_pos)

    @classmethod
    def from_hint_entry(cls, file_id, entry):
        """Return a KeydirEntry from a file_id + HintEntry."""
        return cls(file_id, entry.tstamp, entry.value_sz, entry.value_pos)


def _get_file_id(filename):
    """Return the file_id for this filename."""
    return filename.split('.')[0]


def is_hint(filename):
    """Return True if it's a hint data file."""
    return HINT in filename


def is_live(filename):
    """Return True if it's a live data file."""
    return LIVE in filename


def is_immutable(filename):
    """Return True if it's an immutable data file."""
    return INACTIVE in filename


def is_dead(filename):
    """Return True if it's a dead data file."""
    return DEAD in filename


class WindowsTimer(object):
    """A simmple class to get a more precise timestamp in windows."""

    def __init__(self):
        # start counting from now (and start the clock)
        if hasattr(time, 'clock'):
            self._clock = time.clock
        else:
            self._clock = time.process_time
        self.start = time.time() + self._clock()

    def time(self):
        """Return a float timestamp.

        The timestamp is start_time + time.process_time()/clock().
        """
        return self.start + self._clock()


if sys.platform == 'win32':
    _timer = WindowsTimer()
    timestamp = _timer.time
else:
    timestamp = time.time


class DataFile(object):
    """Class that encapsulates data file handling."""

    last_generated_id = 0

    def __init__(self, base_path, filename=None):
        """Create a DataFile instance.

        If filename is None a new file is created.
        """
        self.has_bad_crc = False
        self.has_bad_data = False
        self.fd = None
        if filename is None:
            filename = self._get_next_file_id() + LIVE + FILE_SUFFIX
        self.filename = os.path.join(base_path, filename)
        self.file_id = _get_file_id(filename)
        self.hint_filename = os.path.join(base_path, filename + HINT)
        self._open()

    @property
    def size(self):
        return os.stat(self.filename).st_size

    @classmethod
    def _get_next_file_id(cls):
        """Return the next file id."""
        generated_id = int(timestamp() * 1000000)
        if generated_id <= cls.last_generated_id:
            logger.warning('Repeated timestamps, probably running under '
                           'VirtualBox. Using workaround.')
            generated_id = cls.last_generated_id + 1
        cls.last_generated_id = generated_id
        return str(generated_id)

    @property
    def has_hint(self):
        """Return true if there is a hint file on-disk."""
        return os.path.exists(self.hint_filename)

    @property
    def hint_size(self):
        """Return the hint file size."""
        if self.has_hint:
            return os.stat(self.hint_filename).st_size
        return 0

    def exists(self):
        return os.path.exists(self.filename)

    def make_immutable(self):
        """Make this data file immutable."""
        self.close()
        new_name = self.filename.replace(LIVE, INACTIVE)
        os.rename(self.filename, new_name)
        return ImmutableDataFile(*os.path.split(new_name))

    def _open(self):
        if not os.path.exists(self.filename):
            # create the file and open it for append
            open(self.filename, 'wb').close()
        self.fd = open(self.filename, 'a+b')

    def close(self):
        """Close the file descriptor"""
        if self.fd:
            self.fd.flush()
            os.fsync(self.fd.fileno())
            self.fd.close()
            self.fd = None

    def iter_entries(self):
        """Return a generator for the entries in the file."""
        fmmap = mmap.mmap(self.fd.fileno(), 0, access=mmap.ACCESS_READ)
        with contextlib.closing(fmmap):
            for entry in self._iter_mmaped_entries(fmmap):
                yield entry

    def _iter_mmaped_entries(self, fmmap):
        """Return a generator for the entries in the mmaped file."""
        current_pos = 0
        while True:
            try:
                entry, new_pos = self.read(fmmap, current_pos)
                current_pos = new_pos
                yield entry
            except EOFError:
                return
            except BadCrc:
                self.has_bad_crc = True
                logger.warning('Found BadCrc on %s at position: %s, '
                               'the rest of the file will be ignored.',
                               self.file_id, current_pos)
                return
            except BadHeader:
                self.has_bad_data = True
                logger.warning('Found corrupted header on %s at position: %s, '
                               'the rest of the file will be ignored.',
                               self.file_id, current_pos)
                return

    def __getitem__(self, item):
        """__getitem__ to support slicing and *only* slicing."""
        if isinstance(item, slice):
            self.fd.seek(item.start)
            return self.fd.read(item.stop - item.start)
        else:
            raise ValueError('Only slice is supported')

    def write(self, row_type, key, value):
        """Write an entry to the file."""
        key_sz = len(key)
        value_sz = len(value)
        tstamp = timestamp()
        header = header_struct.pack(tstamp, key_sz, value_sz, row_type)
        # we do this AND to always have unsigned integer, because of the
        # zlib.crc32 change between Py2 and Py3
        crc32_value = zlib.crc32(header + key + value) & 0xFFFFFFFF
        crc32 = crc32_struct.pack(crc32_value)

        if EXTRA_SEEK:
            # seek to end of file even if we are in append mode, but py2.x IO
            # in win32 is really buggy, see: http://bugs.python.org/issue3207
            self.fd.seek(0, os.SEEK_END)
        self.fd.write(crc32 + header)
        self.fd.write(key)
        self.fd.flush()
        value_pos = self.fd.tell()
        self.fd.write(value)
        self.fd.flush()
        return tstamp, value_pos, value_sz

    def read(self, fmmap, current_pos):
        """Read a single entry from the current position."""
        crc32_bytes = fmmap[current_pos:current_pos + crc32_size]
        current_pos += crc32_size
        header = fmmap[current_pos:current_pos + header_size]
        current_pos += header_size
        if header == b'' or crc32_bytes == b'':
            # reached EOF
            raise EOFError
        try:
            crc32 = crc32_struct.unpack(crc32_bytes)[0]
            tstamp, key_sz, value_sz, row_type = header_struct.unpack(header)
        except struct.error as e:
            raise BadHeader(e)
        key = fmmap[current_pos:current_pos + key_sz]
        current_pos += key_sz
        value_pos = current_pos
        value = fmmap[current_pos:current_pos + value_sz]
        current_pos += value_sz
        # verify the crc32 of the data
        crc32_new_value = zlib.crc32(header + key + value) & 0xFFFFFFFF
        if crc32_new_value == crc32:
            return TritcaskEntry(crc32, tstamp, key_sz, value_sz, row_type,
                                 key, value, value_pos), current_pos
        else:
            raise BadCrc(crc32, crc32_new_value)

    def get_hint_file(self):
        """Open and return the hint file."""
        return HintFile(self.hint_filename)


class ImmutableDataFile(DataFile):
    """An immutable data file."""

    def __init__(self, base_path, filename):
        """Create a ImmutableDataFile instance"""
        self.fmmap = None
        super(ImmutableDataFile, self).__init__(base_path, filename)

    def make_immutable(self):
        """A no-op."""
        return self

    def write(self, *args):
        """Raise a NotImplementedError"""
        raise NotImplementedError

    def make_zombie(self):
        """Rename the file but leave it open.

        Actually close, rename and open it again.
        """
        new_name = self.filename.replace(INACTIVE, DEAD)
        new_hint_name = self.hint_filename.replace(INACTIVE, DEAD)
        self.close()
        os.rename(self.filename, new_name)
        self.filename = new_name
        if self.has_hint:
            os.rename(self.hint_filename, new_hint_name)
            self.hint_filename = new_hint_name
        self._open()
        return self

    def _open(self):
        self.fd = open(self.filename, 'rb')
        fmmap = mmap.mmap(self.fd.fileno(), 0, access=mmap.ACCESS_READ)
        self.fmmap = fmmap

    def close(self):
        """Close the file descriptor and mmap."""
        if self.fmmap:
            self.fmmap.close()
            self.fmmap = None
        # I don't call parent close, as I'm a read-only file and there is no
        # need to call flush nor fsync (and windows hate it and dies in
        # horrible ways)
        if self.fd:
            self.fd.close()
            self.fd = None

    def iter_entries(self):
        """Return a generator for the entries in the mmaped file."""
        for entry in self._iter_mmaped_entries(self.fmmap):
            yield entry

    def __getitem__(self, item):
        """__getitem__ to support slicing and *only* slicing."""
        if isinstance(item, slice):
            return self.fmmap[item]
        else:
            raise ValueError('Only slice is supported')


class DeadDataFile(ImmutableDataFile):
    """A Dead data file."""

    def __init__(self, base_path, filename):
        """Create a DeadDataFile instance."""
        self.filename = os.path.join(base_path, filename)
        self.file_id = _get_file_id(filename)
        self.hint_filename = os.path.join(base_path, filename + HINT)

    def delete(self):
        """Delete this file and the hint if exists."""
        os.unlink(self.filename)
        if self.has_hint:
            os.unlink(self.hint_filename)

    # make all inherited methods to fail
    def _not_implemented(self, *args, **kwargs):
        """raise NotImplementedError."""
        raise NotImplementedError

    _open = close = read = write = make_immutable = make_zombie = \
        __getitem__ = iter_entries = _not_implemented


class TempDataFile(DataFile):
    """A temporary data file."""

    def __init__(self, base_path):
        """Create a TempDataFile instance."""
        self.temp_name = '.' + tempfile.mktemp(dir='')
        fname = self._get_next_file_id() + self.temp_name + FILE_SUFFIX
        super(TempDataFile, self).__init__(base_path, filename=fname)

    def make_immutable(self):
        """Make this data file immutable."""
        self.close()
        new_name = self.filename.replace(self.temp_name, INACTIVE)
        os.rename(self.filename, new_name)
        if self.has_hint:
            new_hint_name = self.hint_filename.replace(self.temp_name, INACTIVE)
            os.rename(self.hint_filename, new_hint_name)
        return ImmutableDataFile(*os.path.split(new_name))

    def delete(self):
        """Delete this file and the hint if exists."""
        self.close()
        os.unlink(self.filename)
        if self.has_hint:
            os.unlink(self.hint_filename)


class HintFile(object):
    """A hint file."""

    def __init__(self, path):
        """Create the instance."""
        self.path = path
        self.tempfile = None
        if os.path.exists(self.path) and os.stat(self.path).st_size > 0:
            # if it's there and size > 0, open only for read
            self.fd = open(self.path, 'rb')
        else:
            # this is a new hint file, lets create it as a tempfile.
            self.tempfile = tempfile.mktemp(dir=os.path.dirname(self.path))
            self.fd = open(self.tempfile, 'w+b')

    def iter_entries(self):
        """Return a generator over the hint entries."""
        fmap = mmap.mmap(self.fd.fileno(), 0, access=mmap.ACCESS_READ)
        with contextlib.closing(fmap):
            current_pos = 0
            while True:
                header = fmap[current_pos:current_pos + hint_header_size]
                current_pos += hint_header_size
                if header == b'':
                    return
                tstamp, key_sz, row_type, value_sz, value_pos = \
                    hint_header_struct.unpack(header)
                key = fmap[current_pos:current_pos + key_sz]
                current_pos += key_sz
                yield HintEntry(tstamp, key_sz, row_type,
                                value_sz, value_pos, key)

    def __enter__(self):
        """Do nothing, the fd should be already opened."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close the fd and flush evetything to disk.

        Also rename the tempfile to the hint filename.
        """
        # make sure the hint data is on disk
        self.close()

    def write(self, entry):
        """Write a hint entry to the file."""
        self.fd.write(hint_header_struct.pack(*entry.header))
        self.fd.write(entry.key)

    def close(self):
        """Close the fd."""
        if self.fd:
            self.fd.flush()
            os.fsync(self.fd.fileno())
            self.fd.close()
        # if this is a new hint file, rename it to the real path.
        if self.tempfile:
            os.rename(self.tempfile, self.path)


class Keydir(dict):
    """The keydir.

    This is basically a dict that keep track of some stats
    like live_bytes/file_id.
    """

    def __init__(self, *args, **kwargs):
        """Create the instance."""
        super(Keydir, self).__init__(*args, **kwargs)
        self._stats = {}

    def __setitem__(self, key, entry):
        """__setitem__ and update the stats."""
        # first update the stats
        stats = self._stats.setdefault(entry.file_id, {})
        try:
            old_entry = self[key]
            if old_entry.file_id != entry.file_id:
                # the prev. entry is from a different file
                # update those stats too!
                old_stats = self._stats[old_entry.file_id]
                old_stats['live_entries'] -= 1
                old_stats['live_bytes'] -= len(key[1]) + old_entry.value_sz \
                    + header_size + crc32_size

                new_bytes = len(key[1]) + entry.value_sz \
                    + header_size + crc32_size
                # update the live entries in this file_id stats
                live_entries = stats.get('live_entries', 0)
                stats['live_entries'] = live_entries + 1
            else:
                new_bytes = entry.value_sz - old_entry.value_sz
        except KeyError:
            # a new entry
            new_bytes = len(key[1]) + entry.value_sz + header_size + crc32_size
            live_entries = stats.get('live_entries', 0)
            stats['live_entries'] = live_entries + 1
        live_bytes = stats.get('live_bytes', 0)
        stats['live_bytes'] = live_bytes + new_bytes
        # add the entry to the keydir
        super(Keydir, self).__setitem__(key, entry)

    def remove(self, key):
        """Remove a key from the keydir and update the stats."""
        # remove it from the keydir and update the stats
        entry = self.pop(key, None)
        # return if we don't have that key
        if entry is None:
            return
        try:
            stats = self._stats[entry.file_id]
            stats['live_bytes'] -= len(key[1]) + entry.value_sz \
                + header_size + crc32_size
            stats['live_entries'] -= 1
        except KeyError as e:
            logger.warning('Failed to update stats while removing %s with: %s',
                           key, e)

    def get_stats(self, file_id):
        """Return a copy of the stats for file_id."""
        return self._stats[file_id].copy()


class Tritcask(object):
    """Implementation of a bitcask-like (row_type,key)/value store.

    The key-value pairs are stored in an "append-only" file, but we also have a
    namespace/row_type.
    Each record has the following format:

    ---------------------------------------------------------------
    |                     header                    |    bytes    |
    ---------------------------------------------------------------
    | crc32 | tstamp | key_sz | value_sz | row_type | key | value |
    ---------------------------------------------------------------
    |   4   |   4    |   4    |    4     |    4     |  X  |   X   |
    ---------------------------------------------------------------

    The on-disk layout is composed of live and inactive files (+ hint files),
    there can be only one live data file and many inactive (immutable) data
    files. Each of the immutable data files might have a hint file to speedup
    the startup, the format of the hint file is:

    -------------------------------------------------------------
    |                     header                        | bytes |
    -------------------------------------------------------------
    | tstamp | key_sz | row_type | value_sz | value_pos |  key  |
    -------------------------------------------------------------
    |    4   |    4   |     4    |    4     |    4      |   X   |
    -------------------------------------------------------------

    The inactive files are compacted/merged into a smaller (also inactive)
    data files in order to keep fragmentation under control.


    This is based on: http://downloads.basho.com/papers/bitcask-intro.pdf
    """

    def __init__(self, path, auto_merge=True, dead_bytes_threshold=0.5,
                 max_immutable_files=20):
        """Initialize the instance.

        @param auto_merge: disable auto merge/compaction.
        @param dead_bytes_threshold: the limit factor of dead vs live bytes to
            trigger a merge and/or live file rotation.
        @param max_immutable_files: the max number of inactive files to use,
            once this value is reached a merge is triggered.
        """
        logger.info("Initializing Tritcask on: %s", path)
        self._keydir = Keydir()
        self.base_path = path
        self.dead_bytes_threshold = dead_bytes_threshold
        self.max_immutable_files = max_immutable_files
        self.auto_merge = auto_merge
        if not os.path.exists(self.base_path):
            os.makedirs(self.base_path)
        elif not os.path.isdir(self.base_path):
            raise ValueError('path must be a directory.')
        self.live_file = None
        self._immutable = {}
        self._find_data_files()
        self._build_keydir()
        # now check if we should rotate the live file
        # and merge immutable ones
        self._rotate_and_merge()
        # check if we found a live data file
        # if not, define one (it will be created later)
        if self.live_file is None:
            # it's a clean start, let's create the first file
            self.live_file = DataFile(self.base_path)

    def shutdown(self):
        """Shutdown and close all open files."""
        logger.info("shutting down...")
        if self.live_file:
            self.live_file.close()
            self.live_file = None
        for data_file in self._immutable.values():
            data_file.close()
        self._immutable.clear()

    def should_rotate(self):
        """Check if we should rotate the live file."""
        # if there is no live file, just say no
        if self.live_file is None:
            return False
        # check if the file is marked with a bad crc
        # as we shouldn't keep adding data to a broken file.
        if self.live_file.has_bad_crc or self.live_file.has_bad_data:
            return True
        # now check the default rotation policy
        try:
            live_file_stats = self._keydir.get_stats(self.live_file.file_id)
        except KeyError:
            # no info for the live file
            return False
        else:
            dead_bytes = live_file_stats['live_bytes'] / self.live_file.size
            return dead_bytes < self.dead_bytes_threshold

    def should_merge(self, immutable_files):
        """Check if the immutable_files should be merged."""
        if not immutable_files:
            return False
        live_bytes = 0
        for file_id in sorted(immutable_files.keys()):
            try:
                stats = self._keydir.get_stats(file_id)
            except KeyError:
                # no stats for this data file
                pass
            else:
                live_bytes += stats['live_bytes']
        total_bytes = sum([f.size for f in immutable_files.values()])
        merge_dead = (live_bytes / total_bytes) < self.dead_bytes_threshold
        # check if we should merge using the max_immutable_files setting.
        if merge_dead or len(self._immutable) > self.max_immutable_files:
            return True
        # shouldn't merge.
        return False

    def _rotate_and_merge(self):
        """Check if we need to rotate/merge the data files and do it."""
        rotated = False
        if self.should_rotate():
            self.rotate(create_file=False)
            rotated = True
        if self.auto_merge:
            # check if we need to merge immutable_files
            if self.should_merge(self._immutable):
                # if the size of the live file
                if self.live_file and self.live_file.size > 0 and not rotated:
                    self.rotate(create_file=False)
                new_file = self.merge(self._immutable)
                if new_file is not None:
                    new_file.close()

    def _find_data_files(self):
        """Collect the files we need to work with."""
        logger.debug("lookingup data files")
        dead_files = 0
        broken_files = 0
        # becuase listdir appends / at the end of the path if there is not
        # os.path.sep at the end we are going to add it, otherwhise on windows
        # we will have wrong paths formed
        base_path = self.base_path
        if not base_path.endswith(os.path.sep):
            base_path += os.path.sep
        files = os.listdir(self.base_path)
        for filename in files:
            # first check for hint and dead
            if is_hint(filename):
                continue
            elif is_dead(filename):
                # a dead file...let's remove it
                dead_files += 1
                DeadDataFile(self.base_path, filename).delete()
                continue
            # if it's a live or immutable file try to open it, but if it's
            # "broken", just rename it and continue
            try:
                if is_live(filename):
                    self.live_file = DataFile(self.base_path, filename)
                elif is_immutable(filename):
                    # it's an immutable file
                    data_file = ImmutableDataFile(self.base_path, filename)
                    self._immutable[data_file.file_id] = data_file
            except IOError as e:
                # oops, failed to open the file..discard it
                broken_files += 1
                orig = os.path.join(self.base_path, filename)
                # get the kind of the file so we can rename it
                kind = LIVE if is_live(filename) else INACTIVE
                dest = orig.replace(kind, BROKEN)
                logger.warning("Failed to open %s, renaming it to: %s - "
                               "error: %s", orig, dest, e)
                # rename it to "broken"
                os.rename(orig, dest)
        # immutable files + live
        logger.info("found %s data files, %s dead and %s broken files",
                    len(self._immutable) + 1, dead_files, broken_files)

    def rotate(self, create_file=True):
        """Rotate the live file only if it already exsits."""
        if self.live_file:
            if self.live_file.exists() and self.live_file.size == 0:
                return
            elif not self.live_file.exists():
                return
            # add the current file to the "immutable" list
            logger.info("rotating live file: %s", self.live_file.filename)
            self.live_file.close()
            data_file = self.live_file.make_immutable()
            self._immutable[data_file.file_id] = data_file
            if create_file:
                # create a new live data file
                self.live_file = DataFile(self.base_path)
            else:
                self.live_file = None

    def _build_keydir(self):
        """Build the keydir."""
        fileids = list(self._immutable.keys())
        if self.live_file:
            fileids.append(self.live_file.file_id)
        logger.debug('building the keydir, using: %s', fileids)
        # sort the files by name, in order to load from older -> newest
        for data_file in sorted(self._immutable.values(),
                                key=attrgetter('filename')):
            if data_file.has_hint and data_file.hint_size > 0:
                # load directly from the hint file, only if the size > 0
                self._load_from_hint(data_file)
            elif data_file.exists() and data_file.size > 0:
                self._load_from_data(data_file)
            else:
                logger.debug('Ignoring empty data file.')
        if self.live_file and self.live_file.exists() and self.live_file.size > 0:
            self._load_from_data(self.live_file)
        else:
            logger.debug('Ignoring empty live file.')
        logger.info('keydir ready! (keys: %d)', len(self._keydir))

    def _load_from_hint(self, data_file):
        """Load keydir contents from a hint file."""
        logger.debug("loading entries from hint of: %s", data_file.filename)
        with data_file.get_hint_file() as hint_file:
            for hint_entry in hint_file.iter_entries():
                if hint_entry.value_pos == TOMBSTONE_POS:
                    self._keydir.remove((hint_entry.row_type, hint_entry.key))
                else:
                    kd_entry = KeydirEntry.from_hint_entry(data_file.file_id,
                                                           hint_entry)
                    self._keydir[(hint_entry.row_type, hint_entry.key)] = kd_entry

    def _load_from_data(self, data_file):
        """Load keydir info from a data file.

        if hint_file != None build the hint file for this data file.
        """
        # build the hint only for immutable_files
        build_hint = not data_file.has_hint and self.live_file != data_file
        logger.debug("loading entries from (build_hint=%s): %s",
                     build_hint, data_file.filename)
        hint_idx = {}
        for entry in data_file.iter_entries():
            # only add it to the index if value isn't TOMBSTONE
            if entry.value == TOMBSTONE:
                # the record is dead, check if need to remove it from
                # the indexes
                self._keydir.remove((entry.row_type, entry.key))
                # add the tombstone entry to the hint
                if build_hint:
                    hint_entry = HintEntry.from_tritcask_entry(entry, dead=True)
                    hint_idx[hint_entry.key] = hint_entry
            else:
                kd_entry = KeydirEntry.from_tritcask_entry(data_file.file_id,
                                                           entry)
                self._keydir[(entry.row_type, entry.key)] = kd_entry
                if build_hint:
                    hint_entry = HintEntry.from_tritcask_entry(entry)
                    hint_idx[hint_entry.key] = hint_entry
        if build_hint and hint_idx:
            # only build the hint file if hint_idx contains data
            with data_file.get_hint_file() as hint_file:
                for key, hint_entry in hint_idx.items():
                    hint_file.write(hint_entry)

    def _get_value(self, file_id, value_pos, value_sz):
        """Get the value for file_id, value_pos."""
        if self.live_file and file_id == self.live_file.file_id:
            # it's the current live file
            return self.live_file[value_pos:value_pos + value_sz]
        else:
            return self._immutable[file_id][value_pos:value_pos + value_sz]

    def put(self, row_type, key, value):
        """Put key/value in the store."""
        # now build the real record
        if not isinstance(key, bytes):
            raise ValueError('key must be a bytes instance.')
        if not isinstance(value, bytes):
            raise ValueError('value must be a bytes instance.')
        tstamp, value_pos, value_sz = self.live_file.write(row_type, key, value)
        if value != TOMBSTONE:
            kd_entry = KeydirEntry(self.live_file.file_id, tstamp,
                                   value_sz, value_pos)
            self._keydir[(row_type, key)] = kd_entry

    def get(self, row_type, key):
        """Get the value for the specified row_type, key."""
        if not isinstance(key, bytes):
            raise ValueError('key must be a bytes instance.')
        kd_entry = self._keydir[(row_type, key)]
        value = self._get_value(kd_entry.file_id, kd_entry.value_pos,
                                kd_entry.value_sz)
        return value

    def keys(self):
        """Return the keys in self._keydir."""
        return self._keydir.keys()

    def __contains__(self, key):
        """Return True if key is in self._keydir."""
        return key in self._keydir

    def delete(self, row_type, key):
        """Delete the key/value specified by key."""
        if not isinstance(key, bytes):
            raise ValueError('key must be a bytes instance.')
        self.put(row_type, key, TOMBSTONE)
        self._keydir.remove((row_type, key))

    def merge(self, immutable_files):
        """Merge a set of immutable files into a single one."""
        logger.info("Starting merge of %s", immutable_files.keys())

        def by_file_id(item):
            file_id = item[1][0]
            return file_id in immutable_files
        filtered_keydir = ifilter(by_file_id, self._keydir.items())
        dest_file = TempDataFile(self.base_path)
        hint_file = dest_file.get_hint_file()
        for keydir_key, kd_entry in sorted(filtered_keydir,
                                           key=lambda item: item[1][0]):
            row_type, key = keydir_key
            value = self._get_value(kd_entry.file_id, kd_entry.value_pos,
                                    kd_entry.value_sz)
            tstamp, value_pos, value_sz = dest_file.write(row_type, key, value)
            hint_entry = HintEntry(tstamp, len(key), row_type, value_sz,
                                   value_pos, key)
            hint_file.write(hint_entry)
        # close and flush files to disk
        hint_file.close()
        dest_file.close()
        if dest_file.size == 0:
            # ooh! all the entries are dead
            # mark immutable_files as zombies and return None
            for ifile in immutable_files.values():
                ifile.make_zombie()
            # also delete the just created dest/tempfile
            dest_file.delete()
            return None
        else:
            new_data_file = dest_file.make_immutable()
            # once we are sure everything is on disk, mark merged files as
            # zombies, so we can still use them
            for ifile in immutable_files.values():
                ifile.make_zombie()
            logger.info("Done merging %s -> %s",
                        immutable_files.keys(), new_data_file.file_id)
            return new_data_file


class TritcaskShelf(DictMixin, object):
    """A shelve.Shelf-like API backed by a tritcask store."""

    def __init__(self, row_type, db):
        """Create the instance."""
        self.row_type = row_type
        self._db = db

    def keys(self):
        """dict protocol."""
        for r, k in self._db.keys():
            if r == self.row_type:
                yield k

    def has_key(self, key):
        """dict protocol."""
        return (self.row_type, key) in self._db.keys()

    def __contains__(self, key):
        """dict protocol."""
        return (self.row_type, key) in self._db

    def __getitem__(self, key):
        """dict protocol."""
        return self._deserialize(self._db.get(self.row_type, key))

    def __iter__(self):
        """MutableMapping protocol (for Py3)."""
        for i in self.keys():
            yield i

    def __setitem__(self, key, value):
        """dict protocol."""
        if not key:
            raise ValueError("Invalid key: %r" % (key,))
        self._db.put(self.row_type, key, self._serialize(value))

    def __delitem__(self, key):
        """dict protocol."""
        self._db.delete(self.row_type, key)

    def __len__(self):
        """The len of the shelf."""
        counter = 0
        # pylint: disable-msg=W0612
        for key in self.keys():
            counter += 1
        return counter

    def _deserialize(self, raw_value):
        """Deserialize the bytes.

        This method allow subclasses to customize the deserialization.
        """
        return pickle.loads(raw_value)

    def _serialize(self, value):
        """Serialize value to string using protocol."""
        return pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
