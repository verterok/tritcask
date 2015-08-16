# tests.test_tritcask - tritcask tests
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
"""Tests for Tritcask and helper functions."""


import contextlib
import mmap
import marshal
import os
import types
import uuid
import zlib

from operator import attrgetter

import tritcask
from tritcask.tritcask import (
    TOMBSTONE,
    LIVE,
    HINT,
    INACTIVE,
    DEAD,
    BROKEN,
    BadCrc,
    BadHeader,
    DataFile,
    ImmutableDataFile,
    TempDataFile,
    DeadDataFile,
    HintFile,
    Tritcask,
    HintEntry,
    KeydirEntry,
    Keydir,
    TritcaskShelf,
    WindowsTimer,
    header_size,
    crc32_size,
    crc32_struct,
    header_struct,
    hint_header_size,
    hint_header_struct,
    _get_file_id,
    is_live,
    is_immutable,
    is_hint,
    time,
    timestamp,
)

from unittest import TestCase
from tests.testcase import BaseTestCase


class DataFileTest(BaseTestCase):
    """Tests for DataFile class."""

    file_class = DataFile

    def test__get_next_file_id(self):
        """Test for _get_next_file_id method."""
        curr = int(timestamp() * 100000)

        self.assertTrue(curr <= int(self.file_class._get_next_file_id()))
        curr = int(timestamp() * 100000)
        self.assertTrue(curr <= int(self.file_class._get_next_file_id()))

    def test_lightning_fast_virtualbox_sometimes_lies(self):
        """Win on Virtualbox sometimes returns 0 in time.clock."""
        test_items = 100
        self.patch(time, "clock", lambda: 0)
        file_ids = [self.file_class._get_next_file_id()
                    for _ in range(test_items)]
        # even though the clock is not moving, all ids must be different!
        self.assertEqual(len(set(file_ids)), test_items)

    def test_log_warning_repeated_timestamps(self):
        """When the clock is not moving, we log a warning."""
        expected_warning = "Repeated timestamps"
        ts = timestamp() + 1.0
        self.patch(tritcask.tritcask, "timestamp", lambda: ts)

        id1 = self.file_class._get_next_file_id()
        self.assertFalse(self.memento.check_warning(expected_warning))

        id2 = self.file_class._get_next_file_id()
        self.assertNotEqual(id1, id2)
        self.assertTrue(self.memento.check_warning(expected_warning))

    def test_exists(self):
        """Tests for exists method."""
        new_file = self.file_class(self.base_dir)
        self.assertTrue(new_file.exists())
        self.assertTrue(os.path.exists(new_file.filename))

    def test_size(self):
        """Test the size property."""
        new_file = self.file_class(self.base_dir)
        self.assertEqual(0, new_file.size)
        new_file.fd.write(b'foo')
        new_file.fd.flush()
        self.assertEqual(len(b'foo'), new_file.size)

    def test_has_hint(self):
        """Test that has_hint works as expetced."""
        new_file = self.file_class(self.base_dir)
        self.assertFalse(new_file.has_hint)

    def test_hint_size(self):
        """Test that hint_size work as expected."""
        new_file = self.file_class(self.base_dir)
        self.assertEqual(new_file.hint_size, 0)

    def test__open(self):
        """Test the _open private method."""
        new_file = self.file_class(self.base_dir)
        # check that the file is opened
        new_file.fd.write(b'foo')
        new_file.close()

    def test_close(self):
        """Test the close method."""
        new_file = self.file_class(self.base_dir)
        new_file.close()
        self.assertEqual(None, new_file.fd)

    def test_make_immutable(self):
        """Test for make_immutable method."""
        new_file = self.file_class(self.base_dir)
        new_file.fd.write(b'foo')
        new_file.fd.flush()
        immutable_file = new_file.make_immutable()
        # the DataFile should be closed
        self.assertEqual(None, new_file.fd)
        self.assertEqual(immutable_file.file_id, new_file.file_id)

    def test_iter_entries(self):
        """Test for iter_entries"""
        db = Tritcask(self.base_dir)
        self.addCleanup(db.shutdown)
        for i in range(10):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        for i, entry in enumerate(db.live_file.iter_entries()):
            self.assertEqual(entry[4], i)
            self.assertEqual(entry[5], ('foo%d' % (i,)).encode('ascii'))
            self.assertEqual(entry[6], ('bar%d' % (i,)).encode('ascii'))

    def test_iter_entries_bad_crc(self):
        """Test that BadCrc during iter_entries is the same as EOF."""
        db = Tritcask(self.base_dir)
        self.addCleanup(db.shutdown)
        for i in range(10):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        # write a different value -> random bytes
        # now write some garbage to the end of file
        db.live_file.fd.write(os.urandom(100))
        db.live_file.fd.flush()
        # and add 10 new entries
        for i in range(10, 20):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        entries = []
        for i, entry in enumerate(db.live_file.iter_entries()):
            entries.append(entry)
            self.assertEqual(entry[4], i)
            self.assertEqual(entry[5], ('foo%d' % (i,)).encode('ascii'))
            self.assertEqual(entry[6], ('bar%d' % (i,)).encode('ascii'))
        self.assertEqual(10, len(entries))
        self.assertTrue(self.memento.check_warning('Found BadCrc on'))
        self.assertTrue(self.memento.check_warning(
            'the rest of the file will be ignored.'))
        self.assertTrue(db.live_file.has_bad_crc, 'has_bad_crc should be True.')

    def test_iter_entries_bad_header_unpack(self):
        """Test that unpack error during iter_entries is the same as EOF."""
        db = Tritcask(self.base_dir)
        self.addCleanup(db.shutdown)
        for i in range(10):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        # truncate the file at the start of the header of the last value.
        curr_pos = db.live_file.fd.tell()
        db.live_file.fd.seek(curr_pos - header_size + 4)
        db.live_file.fd.truncate()
        entries = []
        for i, entry in enumerate(db.live_file.iter_entries()):
            entries.append(entry)
            self.assertEqual(entry[4], i)
            self.assertEqual(entry[5], ('foo%d' % (i,)).encode('ascii'))
            self.assertEqual(entry[6], ('bar%d' % (i,)).encode('ascii'))
        self.assertEqual(9, len(entries))
        self.assertTrue(db.live_file.has_bad_data, 'has_bad_data should be True.')
        self.assertTrue(self.memento.check_warning('Found corrupted header on'))
        self.assertTrue(self.memento.check_warning(
            'the rest of the file will be ignored.'))

    def test__getitem__(self):
        """Test our slicing support."""
        data_file = self.file_class(self.base_dir)
        tstamp, value_pos, value_sz = data_file.write(0, b'foo', b'bar')
        tstamp1, value1_pos, value1_sz = data_file.write(1, b'foo1', b'bar1')
        self.assertEqual(b'bar', data_file[value_pos:value_pos + value_sz])
        self.assertEqual(b'bar1', data_file[value1_pos:value1_pos + value1_sz])

    def test__getitem__no_slice(self):
        """Test that we *only* support slicing."""
        data_file = self.file_class(self.base_dir)
        tstamp, value_pos, value_sz = data_file.write(0, b'foo', b'bar')
        self.assertRaises(ValueError, data_file.__getitem__, value_pos)

    def test_write(self):
        """Test for write method."""
        data_file = self.file_class(self.base_dir)
        tstamp_1, value_1_pos, value_1_sz = data_file.write(0, b'foo', b'bar')
        self.assertEqual(len(b'bar'), value_1_sz)
        tstamp_2, value_2_pos, value_2_sz = data_file.write(1, b'foo1', b'bar1')
        self.assertEqual(len(b'bar1'), value_2_sz)
        data_file.close()
        # check that the entry is in the file
        with open(data_file.filename, 'rb') as f:
            raw_data_len = (len(b'foo') + len(b'bar') + crc32_size + header_size)
            raw_data = f.read(raw_data_len)
            crc32 = crc32_struct.unpack(raw_data[:crc32_size])[0]
            raw_header = raw_data[crc32_size:crc32_size + header_size]
            header = header_struct.unpack(raw_header)
            tstamp, key_sz, value_sz, row_type = header
            value = raw_data[crc32_size + header_size + key_sz:]
            self.assertEqual(zlib.crc32(raw_header + b'foo' + b'bar') & 0xFFFFFFFF, crc32)
            self.assertEqual(len(b'foo'), key_sz)
            self.assertEqual(len(b'bar'), value_sz)
            self.assertEqual(b'bar', value)
            self.assertEqual(0, row_type)

    def test_read(self):
        """Test for read method."""
        data_file = self.file_class(self.base_dir)
        orig_tstamp, _, _ = data_file.write(0, b'foo', b'bar')
        tstamp1, _, _ = data_file.write(1, b'foo1', b'bar1')
        fmap = mmap.mmap(data_file.fd.fileno(), 0, access=mmap.ACCESS_READ)
        with contextlib.closing(fmap):
            current_pos = 0
            file_data, new_pos = data_file.read(fmap, current_pos)
            crc32, tstamp, key_sz, value_sz, row_type, key, value, pos = file_data
            current_pos = new_pos
            self.assertEqual(crc32_size + header_size + key_sz + value_sz, new_pos)
            self.assertEqual(orig_tstamp, tstamp)
            self.assertEqual(len(b'foo'), key_sz)
            self.assertEqual(len(b'bar'), value_sz)
            self.assertEqual(b'bar', value)
            self.assertEqual(0, row_type)
            file_info, new_pos = data_file.read(fmap, current_pos)
            crc32, tstamp, key_sz, value_sz, row_type, key, value, pos = file_info
            self.assertEqual(crc32_size + header_size + key_sz + value_sz + current_pos, new_pos)
            self.assertEqual(tstamp1, tstamp)
            self.assertEqual(len(b'foo1'), key_sz)
            self.assertEqual(len(b'bar1'), value_sz)
            self.assertEqual(b'bar1', value)
            self.assertEqual(1, row_type)
        data_file.close()

    def test_read_bad_crc(self):
        """Test for read method with a bad crc error."""
        data_file = self.file_class(self.base_dir)
        orig_tstamp, _, _ = data_file.write(0, b'foo', b'bar')
        # mess with the data on disk to make this entry crc32 invalid
        # seek to the end of crc32+header+key
        data_file.close()
        with open(data_file.filename, 'r+b') as fd:
            fd.seek(crc32_size + header_size + len(b'foo'))
            # write a different value -> random bytes
            fd.write(os.urandom(len(b'bar')))
            fd.flush()
            fmap = mmap.mmap(fd.fileno(), 0, access=mmap.ACCESS_READ)
            with contextlib.closing(fmap):
                self.assertRaises(BadCrc, data_file.read, fmap, 0)

    def test_read_bad_header(self):
        """Test for read method with a bad header/unpack error."""
        data_file = self.file_class(self.base_dir)
        orig_tstamp, _, _ = data_file.write(0, b'foo', b'bar')
        # mess with the data on disk to make this entry crc32 invalid
        # seek to the end of crc32+header+key
        data_file.close()
        with open(data_file.filename, 'r+b') as fd:
            fd.read(crc32_size + 4)
            fd.truncate()
            # write a different value -> random bytes
            fd.write(os.urandom(header_size // 2))
            fd.flush()
            fmap = mmap.mmap(fd.fileno(), 0, access=mmap.ACCESS_READ)
            with contextlib.closing(fmap):
                self.assertRaises(BadHeader, data_file.read, fmap, 0)

    def test_write_after_read_after_write(self):
        """write data after a write/read cycle."""
        data1 = os.urandom(200)
        data_file = self.file_class(self.base_dir)
        init_pos = data_file.fd.tell()
        tstamp_1, value_1_pos, value_1_sz = data_file.write(0, b'foo', data1)
        header = header_struct.pack(tstamp_1, len(b'foo'), len(data1), 0)
        crc32 = crc32_struct.pack(zlib.crc32(header + b'foo' + data1) & 0xFFFFFFFF)
        self.assertEqual(init_pos + len(crc32 + header) + len(b'foo'),
                         value_1_pos)
        init_pos_2 = value_1_pos + len(data1)
        data2 = os.urandom(100)
        tstamp_2, value_2_pos, value_2_sz = data_file.write(0, b'foo1', data2)
        header = header_struct.pack(tstamp_2, len(b'foo1'), len(data2), 0)
        crc32 = crc32_struct.pack(zlib.crc32(header + b'foo1' + data2) & 0xFFFFFFFF)
        self.assertEqual(init_pos_2 + len(crc32 + header) + len(b'foo1'),
                         value_2_pos)
        # now read the first value
        value_1 = data_file[value_1_pos:value_1_pos + value_1_sz]
        self.assertEqual(value_1, data1)
        # now write something else, should end up at the end.
        init_pos_3 = value_2_pos + len(data2)
        data3 = os.urandom(100)
        tstamp_3, value_3_pos, value_3_sz = data_file.write(0, b'foo2', data3)
        header = header_struct.pack(tstamp_3, len(b'foo2'), len(data3), 0)
        crc32 = crc32_struct.pack(zlib.crc32(header + b'foo2' + data3) & 0xFFFFFFFF)
        self.assertEqual(init_pos_3 + len(crc32 + header) + len(b'foo2'),
                         value_3_pos)


class TempDataFileTest(DataFileTest):
    """Tests for TempDataFile."""

    file_class = TempDataFile

    def test_tempfile_name(self):
        """Test the name of the tempfile isn't LIVE."""
        new_file = self.file_class(self.base_dir)
        self.assertNotIn(LIVE, new_file.filename)

    def test_make_immutable_and_rename_hint(self):
        """Test for make_immutable method."""
        new_file = self.file_class(self.base_dir)
        new_file.fd.write(b'foo')
        with new_file.get_hint_file() as hint_file:
            hint_file.fd.write(b'foo,bar')
        immutable_file = new_file.make_immutable()
        # the DataFile should be closed
        self.assertEqual(None, new_file.fd)
        self.assertEqual(immutable_file.file_id, new_file.file_id)
        # and the hint should have the new name too
        self.assertTrue(os.path.exists(immutable_file.get_hint_file().path))
        self.assertFalse(os.path.exists(new_file.get_hint_file().path))

    def test_delete(self):
        """Test for delete method."""
        new_file = self.file_class(self.base_dir)
        self.assertTrue(os.path.exists(new_file.filename))
        with new_file.get_hint_file():
            pass
        self.assertTrue(os.path.exists(new_file.hint_filename))
        new_file.delete()
        self.assertFalse(os.path.exists(new_file.filename))
        self.assertFalse(os.path.exists(new_file.hint_filename))


class ImmutableDataFileTest(DataFileTest):

    def test_make_immutable(self):
        """Test for make_immutable."""
        new_file = DataFile(self.base_dir)
        # write some data
        new_file.fd.write(b'foo')
        immutable_file = new_file.make_immutable()
        # it's the same instance?
        self.assertEqual(immutable_file.make_immutable(), immutable_file)

    def test_make_zombie(self):
        """Test for the make_zombie method."""
        new_file = DataFile(self.base_dir)
        # write some data
        new_file.fd.write(b'foo')
        immutable_file = new_file.make_immutable()
        # create a zombie
        zombie = immutable_file.make_zombie()
        # it's the same instance?
        self.assertEqual(zombie, immutable_file)
        self.assertFalse(zombie.fd.closed)

    def test_make_zombie_with_hint(self):
        """Test for the make_zombie method with a hint file.."""
        new_file = DataFile(self.base_dir)
        # write some data
        new_file.fd.write(b'foo')
        immutable_file = new_file.make_immutable()
        immutable_file.get_hint_file().close()
        # create a zombie
        zombie = immutable_file.make_zombie()
        self.assertTrue(zombie.has_hint)
        # it's the same instance?
        self.assertEqual(zombie, immutable_file)
        self.assertFalse(zombie.fd.closed)

    def test_write(self):
        """Test the write fails on immutable files."""
        new_file = DataFile(self.base_dir)
        # write some data
        new_file.fd.write(b'foo')
        immutable_file = new_file.make_immutable()
        self.assertRaises(NotImplementedError, immutable_file.write, 0, b'foo', b'bar')

    def test__open(self):
        """Test the _open private method."""
        new_file = DataFile(self.base_dir)
        # write some data
        new_file.fd.write(b'foo')
        immutable_file = new_file.make_immutable()
        self.assertTrue(immutable_file.fd is not None)
        self.assertTrue(immutable_file.fmmap is not None)
        # check that the file is opened only for read
        self.assertRaises(IOError, immutable_file.fd.write, b'foo')
        immutable_file.close()

    def test_close(self):
        """Test the close method."""
        new_file = DataFile(self.base_dir)
        # write some data
        new_file.fd.write(b'foo')
        immutable_file = new_file.make_immutable()
        immutable_file.close()
        self.assertEqual(None, immutable_file.fd)
        self.assertEqual(None, immutable_file.fmmap)

    def test_iter_entries(self):
        """Test for iter_entries"""
        db = Tritcask(self.base_dir)
        for i in range(10):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        file_id = db.live_file.file_id
        db.rotate()
        db.shutdown()
        db = Tritcask(self.base_dir)
        self.addCleanup(db.shutdown)
        for i, entry in enumerate(db._immutable[file_id].iter_entries()):
            self.assertEqual(entry[4], i)
            self.assertEqual(entry[5], ('foo%d' % (i,)).encode('ascii'))
            self.assertEqual(entry[6], ('bar%d' % (i,)).encode('ascii'))

    def test_iter_entries_bad_crc(self):
        """Test that BadCrc during iter_entries is the same as EOF."""
        db = Tritcask(self.base_dir)
        for i in range(10):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        # write a different value -> random bytes
        # now write some garbage to the end of file
        db.live_file.fd.write(os.urandom(100))
        db.live_file.fd.flush()
        # and add 10 new entries
        for i in range(10, 20):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        file_id = db.live_file.file_id
        db.rotate()
        db.shutdown()
        db = Tritcask(self.base_dir)
        self.addCleanup(db.shutdown)
        entries = []
        for i, entry in enumerate(db._immutable[file_id].iter_entries()):
            entries.append(entry)
            self.assertEqual(entry[4], i)
            self.assertEqual(entry[5], ('foo%d' % (i,)).encode('ascii'))
            self.assertEqual(entry[6], ('bar%d' % (i,)).encode('ascii'))
        self.assertEqual(10, len(entries))
        self.assertTrue(self.memento.check_warning('Found BadCrc on'))
        self.assertTrue(self.memento.check_warning(
            'the rest of the file will be ignored.'))

    def test_iter_entries_bad_header_unpack(self):
        """Test that unpack error during iter_entries is the same as EOF."""
        db = Tritcask(self.base_dir)
        for i in range(10):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        # truncate the file at the start of the header of the last value.
        db.live_file.fd.seek(crc32_size + header_size + 8 + crc32_size + 4)
        db.live_file.fd.truncate()
        file_id = db.live_file.file_id
        db.rotate()
        db.shutdown()
        db = Tritcask(self.base_dir)
        self.addCleanup(db.shutdown)
        entries = []
        for i, entry in enumerate(db._immutable[file_id].iter_entries()):
            entries.append(entry)
            self.assertEqual(entry[4], i)
            self.assertEqual(entry[5], ('foo%d' % (i,)).encode('ascii'))
            self.assertEqual(entry[6], ('bar%d' % (i,)).encode('ascii'))
        self.assertEqual(1, len(entries))
        self.assertTrue(db._immutable[file_id].has_bad_data,
                        'has_bad_data should be True.')
        self.assertTrue(self.memento.check_warning('Found corrupted header on'))
        self.assertTrue(self.memento.check_warning(
            'the rest of the file will be ignored.'))

    def test__getitem__(self):
        """Test our slicing support."""
        rw_file = DataFile(self.base_dir)
        tstamp, value_pos, value_sz = rw_file.write(0, b'foo', b'bar')
        tstamp1, value1_pos, value1_sz = rw_file.write(1, b'foo1', b'bar1')
        data_file = rw_file.make_immutable()
        self.assertEqual(b'bar', data_file[value_pos:value_pos + value_sz])
        self.assertEqual(b'bar1', data_file[value1_pos:value1_pos + value1_sz])

    def test__getitem__no_slice(self):
        """Test that we *only* support slicing."""
        rw_file = DataFile(self.base_dir)
        tstamp, value_pos, value_sz = rw_file.write(0, b'foo', b'bar')
        data_file = rw_file.make_immutable()
        self.assertRaises(ValueError, data_file.__getitem__, value_pos)

    def test_exists(self):
        """Tests for exists method."""
        rw_file = DataFile(self.base_dir)
        tstamp, value_pos, value_sz = rw_file.write(0, b'foo', b'bar')
        data_file = rw_file.make_immutable()
        self.assertTrue(data_file.exists())

    def test_size(self):
        """Test the size property."""
        rw_file = DataFile(self.base_dir)
        tstamp, value_pos, value_sz = rw_file.write(0, b'foo', b'bar')
        data_file = rw_file.make_immutable()
        self.assertEqual(len(b'bar') + len(b'foo') + header_size + crc32_size,
                         data_file.size)

    def test_has_hint(self):
        """Test that has_hint works as expetced."""
        rw_file = DataFile(self.base_dir)
        tstamp, value_pos, value_sz = rw_file.write(0, b'foo', b'bar')
        data_file = rw_file.make_immutable()
        self.assertFalse(data_file.has_hint)
        data_file.get_hint_file().close()
        self.assertTrue(data_file.has_hint)

    def test_hint_size(self):
        """Test that hint_size work as expected."""
        rw_file = DataFile(self.base_dir)
        tstamp, value_pos, value_sz = rw_file.write(0, b'foo', b'bar')
        data_file = rw_file.make_immutable()
        self.assertEqual(data_file.hint_size, 0)
        hint_file = data_file.get_hint_file()
        hint_file.fd.write(b"some data")
        hint_file.close()
        self.assertEqual(data_file.hint_size, len(b"some data"))


class DeadDataFileTest(ImmutableDataFileTest):
    """Tests for DeadDataFile."""

    file_class = DeadDataFile

    def create_dead_file(self):
        """Helper method to create a dead file."""
        rw_file = DataFile(self.base_dir)
        tstamp, value_pos, value_sz = rw_file.write(0, b'foo', b'bar')
        immutable_file = rw_file.make_immutable()
        immutable_file.get_hint_file().close()
        data_file = immutable_file.make_zombie()
        data_file.close()
        return DeadDataFile(self.base_dir, os.path.basename(data_file.filename))

    def test_delete(self):
        """Test for delete method."""
        dead_file = self.create_dead_file()
        self.assertTrue(os.path.exists(dead_file.filename))
        self.assertTrue(os.path.exists(dead_file.hint_filename))
        dead_file.delete()
        self.assertFalse(os.path.exists(dead_file.filename))
        self.assertFalse(os.path.exists(dead_file.hint_filename))

    def test_write(self):
        """Test that write always fails with NotImplementedError."""
        dead_file = self.create_dead_file()
        self.assertRaises(NotImplementedError, dead_file.write, b'1')

    test_write_after_read_after_write = test_write

    def test_read(self):
        """Test that read always fails with NotImplementedError."""
        dead_file = self.create_dead_file()
        self.assertRaises(NotImplementedError, dead_file.read)

    test_read_bad_crc = test_read
    test_read_bad_header = test_read

    def test__open(self):
        """Test that always fails with NotImplementedError."""
        dead_file = self.create_dead_file()
        self.assertRaises(NotImplementedError, dead_file._open)

    def test_close(self):
        """Test that always fails with NotImplementedError."""
        dead_file = self.create_dead_file()
        self.assertRaises(NotImplementedError, dead_file.close)

    def test_make_immutable(self):
        """Test that always fails with NotImplementedError."""
        dead_file = self.create_dead_file()
        self.assertRaises(NotImplementedError, dead_file.make_immutable)

    def test_make_zombie(self):
        """Test that always fails with NotImplementedError."""
        dead_file = self.create_dead_file()
        self.assertRaises(NotImplementedError, dead_file.make_zombie)

    test_make_zombie_with_hint = test_make_zombie

    def test__getitem__(self):
        """Test that always fails with NotImplementedError."""
        dead_file = self.create_dead_file()
        self.assertRaises(NotImplementedError, dead_file.__getitem__)

    test__getitem__no_slice = test__getitem__

    def test_iter_entries(self):
        """Test that always fails with NotImplementedError."""
        dead_file = self.create_dead_file()
        self.assertRaises(NotImplementedError, dead_file.iter_entries)

    def test_exists(self):
        """Tests for exists method."""
        dead_file = self.create_dead_file()
        self.assertTrue(dead_file.exists())

    def test_size(self):
        """Test the size property."""
        dead_file = self.create_dead_file()
        self.assertEqual(len(b'bar') + len(b'foo') + header_size + crc32_size,
                         dead_file.size)


class HintFileTest(BaseTestCase):
    """Tests for HintFile class."""

    def test_init(self):
        """Test initialization."""
        path = os.path.join(self.base_dir, 'test_hint')
        hint_file = HintFile(path)
        # in Py3 if you open w+b it will have mode rb+ (which is ok)
        self.assertIn(hint_file.fd.mode, ('w+b', 'rb+'))

    def test_init_existing(self):
        """Test initialization with existing file."""
        path = os.path.join(self.base_dir, 'test_hint_existing')
        hint_file = HintFile(path)
        hint_file.fd.write(b"some data")
        hint_file.close()
        hint_file = HintFile(path)
        self.assertEqual('rb', hint_file.fd.mode)

    def test_init_existing_empty(self):
        """Test initialization with existing file."""
        path = os.path.join(self.base_dir, 'test_hint_existing')
        hint_file = HintFile(path)
        hint_file.close()
        hint_file = HintFile(path)
        # in Py3 if you open w+b it will have mode rb+ (which is ok)
        self.assertIn(hint_file.fd.mode, ('w+b', 'rb+'))

    def test_close(self):
        """Test for the close method."""
        path = os.path.join(self.base_dir, 'test_hint_close')
        hint_file = HintFile(path)
        # in Py3 if you open w+b it will have mode rb+ (which is ok)
        self.assertIn(hint_file.fd.mode, ('w+b', 'rb+'))
        fd = hint_file.fd
        hint_file.close()
        self.assertTrue(fd.closed)

    def test_contextmanager(self):
        """Test the context manager protocol."""
        path = os.path.join(self.base_dir, 'test_hint_close')
        hint_file = HintFile(path)
        with hint_file as hf:
            self.assertEqual(hint_file, hf)

    def test_iter_entries(self):
        """Test for iter_entries method."""
        db = Tritcask(self.base_dir)
        # create some stuff.
        for i in range(10):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%d' % (i,)).encode('ascii'))
        # make the data file inactive and generate the hint.
        immutable_file = db.live_file.make_immutable()
        db.shutdown()
        Tritcask(self.base_dir).shutdown()
        self.assertTrue(immutable_file.has_hint)
        # check that the hint matches the contents in the DB
        hint_file = immutable_file.get_hint_file()
        db = Tritcask(self.base_dir)
        self.addCleanup(db.shutdown)
        for hint_entry in hint_file.iter_entries():
            self.assertEqual(6, len(hint_entry))
            self.assertTrue(db.get(hint_entry[2], hint_entry[-1]) is not None)

    def test_write(self):
        """Test the write method."""
        hint_file = HintFile(os.path.join(self.base_dir, 'hint_file'))
        tstamp1 = timestamp()
        entry = HintEntry(tstamp1, len(b'foo'), 0, len(b'bar'), 100, b'foo')
        hint_file.write(entry)
        tstamp2 = timestamp()
        entry2 = HintEntry(tstamp2, len(b'foo1'), 1, len(b'bar1'), 100, b'foo1')
        hint_file.write(entry2)
        hint_file.close()
        # check that the entry is in the file
        with open(hint_file.path, 'rb') as f:
            header = hint_header_struct.unpack(f.read(hint_header_size))
            tstamp, key_sz, row_type, value_sz, value_pos = header
            self.assertEqual(tstamp1, tstamp)
            self.assertEqual(len(b'foo'), key_sz)
            self.assertEqual(0, row_type)
            self.assertEqual(len(b'bar'), value_sz)
            self.assertEqual(100, value_pos)
            key = f.read(key_sz)
            self.assertEqual(b'foo', key)
            # read the second entry
            header = hint_header_struct.unpack(f.read(hint_header_size))
            tstamp, key_sz, row_type, value_sz, value_pos = header
            self.assertEqual(tstamp2, tstamp)
            self.assertEqual(len('foo1'), key_sz)
            self.assertEqual(1, row_type)
            self.assertEqual(len('bar1'), value_sz)
            self.assertEqual(100, value_pos)
            key = f.read(key_sz)
            self.assertEqual(b'foo1', key)


class HintEntryTest(BaseTestCase):
    """Tests for HintEntry class."""

    def test_header_property(self):
        """Test the header property."""
        tstamp = timestamp()
        entry = HintEntry(tstamp, len('foo'), 0, len('bar'), 100, 'foo')
        self.assertEqual((entry.tstamp, entry.key_sz, entry.row_type,
                          entry.value_sz, entry.value_pos), entry.header)


class LowLevelTest(BaseTestCase):
    """Tests for low level methods and functions.

    Test the on-disk format, writes and reads.
    """

    def setUp(self):
        super(LowLevelTest, self).setUp()
        self.db = Tritcask(self.base_dir)
        self.addCleanup(self.db.shutdown)

    def test_get_file_id(self):
        """Test for _get_file_id function."""
        filename = os.path.split(self.db.live_file.filename)[1]
        self.assertEqual(self.db.live_file.file_id, _get_file_id(filename))

    def test_is_hint(self):
        """Test for is_hint function."""
        filename = DataFile._get_next_file_id() + LIVE
        self.assertFalse(is_hint(filename))
        filename = DataFile._get_next_file_id() + HINT
        self.assertTrue(is_hint(filename))

    def test_is_immutable(self):
        """Test for is_immutable function."""
        filename = DataFile._get_next_file_id() + LIVE
        self.assertFalse(is_immutable(filename))
        filename = DataFile._get_next_file_id() + INACTIVE
        self.assertTrue(is_immutable(filename))

    def test_is_live(self):
        """Test for is_live function."""
        filename = DataFile._get_next_file_id() + HINT
        self.assertFalse(is_live(filename))
        filename = DataFile._get_next_file_id() + LIVE
        self.assertTrue(is_live(filename))

    def test_get_value(self):
        """Test _get_value method."""
        self.db.put(0, b'foo', b'bar')
        value = self.db._get_value(
            self.db.live_file.file_id, crc32_size + header_size + len(b'foo'),
            len(b'bar'))
        self.assertEqual(value, b'bar')

    def test_get_value_different_file_ids(self):
        """Test _get_value with different file_id."""
        self.db.put(0, b'foo', b'bar')
        old_file_id = self.db.live_file.file_id
        # shutdown and rename the file.
        self.db.live_file.make_immutable()
        self.db.shutdown()
        self.db = Tritcask(self.base_dir)
        self.addCleanup(self.db.shutdown)
        self.db.put(1, b'foo1', b'bar1')
        self.assertEqual(1, len(self.db._immutable))
        # read from the old file
        value = self.db._get_value(
            old_file_id, crc32_size + header_size + len(b'foo'), len(b'bar'))
        self.assertEqual(value, b'bar')
        # read from the current file
        value = self.db._get_value(self.db.live_file.file_id,
                                   crc32_size + header_size + len(b'foo1'),
                                   len(b'bar1'))
        self.assertEqual(value, b'bar1')

    def test_shutdown(self):
        """Test shutdown."""
        # create 1 inactive files, with 5 items each
        for i in range(5):
            self.db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        # make the data file inactive and generate the hint.
        self.db.live_file.make_immutable()
        self.db.shutdown()
        self.db = Tritcask(self.base_dir)
        for i in range(5, 10):
            self.db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        # iterate over all values, in order to open all inactive data
        # files.
        for rtype, key in self.db._keydir.keys():
            self.db.get(rtype, key)
        # check that we have 2 open files
        try:
            self.assertEqual(1, len(self.db._immutable))
            self.assertNotEqual(None, self.db.live_file)
        finally:
            self.db.shutdown()
        self.assertEqual(None, self.db.live_file)
        self.assertEqual(0, len(self.db._immutable))

    def test_rotate_files(self):
        """Test data file rotation."""
        # add a key/value
        self.db.put(0, b'foo', b'bar')
        files = sorted(os.listdir(self.db.base_path))
        self.assertEqual(1, len(files), files)
        self.db.rotate()
        files = sorted(os.listdir(self.db.base_path))
        self.assertEqual(2, len(files), files)
        self.assertIn(INACTIVE, files[0])
        self.assertIn(LIVE, files[1])
        # add a new value to trigger the creation of the new file
        self.db.put(0, b'foo', b'bar1')
        self.assertEqual(2, len(files), files)
        self.assertIn(INACTIVE, files[0])
        self.assertIn(LIVE, files[1])

    def test_rotate_empty_live_file(self):
        """Test data file rotation with empty live file."""
        self.db.rotate()
        files = sorted(os.listdir(self.db.base_path))
        self.assertEqual(1, len(files), files)
        self.assertIn(LIVE, files[0])
        self.assertEqual(self.db.live_file.filename,
                         os.path.join(self.db.base_path, files[0]))


class InitTest(BaseTestCase):
    """Init tests."""

    def test_initialize_new(self):
        """Simple initialization."""
        os.rmdir(self.base_dir)
        db = Tritcask(self.base_dir)
        db.shutdown()

    def test_initialize_existing_empty(self):
        """Initialize with an empty existing db."""
        db = Tritcask(self.base_dir)
        db.shutdown()
        db = Tritcask(self.base_dir)
        db.shutdown()

    def test_initialize_existing(self):
        """Initialize with a existing db."""
        db = Tritcask(self.base_dir)
        for i in range(10):
            key, value = self.build_data()
            db.put(0, key, value)
        db.shutdown()
        db = Tritcask(self.base_dir)
        self.addCleanup(db.shutdown)
        self.assertTrue(self.memento.check_debug(
            'loading entries from (build_hint=%s): %s'
            % (False, db.live_file.filename)))

    def test_initialize_bad_path(self):
        """Initialize with a invalid path."""
        path = os.path.join(self.base_dir, 'foo')
        open(path, 'w').close()
        self.assertRaises(ValueError, Tritcask, path)

    def test_find_data_files(self):
        """Test the _find_data_files method."""
        filenames = []
        for i in range(10):
            data_file = DataFile(self.base_dir)
            data_file.write(i, ('foo%d' % (i,)).encode('ascii'), ('bar%d' % (i,)).encode('ascii'))
            immutable_file = data_file.make_immutable()
            filenames.append(immutable_file.filename)
            immutable_file.close()
        db = Tritcask(self.base_dir)
        self.addCleanup(db.shutdown)
        self.assertNotIn(db.live_file.filename, filenames)
        files = [fn.filename for fn in sorted(db._immutable.values(),
                                              key=attrgetter('filename'))]
        self.assertEqual(files, filenames)

    def test_find_data_files_immutable_open_error(self):
        """Test the _find_data_files method failing to open a file."""
        data_file = DataFile(self.base_dir)
        data_file.write(0, b'foo_0', b'bar_0')
        immutable_file = data_file.make_immutable()
        immutable_file.close()

        # patch the open call and make it fail
        def fail_open(filename, *a):
            """Always fail."""
            raise IOError("I'm a broken file.")
        self.patch(ImmutableDataFile, '_open', fail_open)
        db = Tritcask(self.base_dir)
        self.addCleanup(db.shutdown)
        files = [fn.filename for fn in sorted(db._immutable.values(),
                                              key=attrgetter('filename'))]
        self.assertNotIn(immutable_file.filename, files)
        # check the logs
        msg = ("Failed to open %s, renaming it to: %s - error: %s" %
               (immutable_file.filename,
                immutable_file.filename.replace(INACTIVE, BROKEN),
                IOError("I'm a broken file")))
        self.assertTrue(self.memento.check_warning(msg))

    def test_find_data_files_live_open_error(self):
        """Test the _find_data_files method failing to open a file."""
        data_file = DataFile(self.base_dir)
        data_file.write(0, b'foo_0', b'bar_0')
        data_file.close()
        # patch the open call and make it fail
        orig_open = DataFile._open

        def fail_open(filename, *a):
            """Always fail only once."""
            self.patch(DataFile, '_open', orig_open)
            raise IOError("I'm a broken file.")
        self.patch(DataFile, '_open', fail_open)
        db = Tritcask(self.base_dir)
        self.addCleanup(db.shutdown)
        self.assertNotEqual(data_file.filename, db.live_file.filename)
        # check the logs
        self.memento.debug = True
        msg = ("Failed to open %s, renaming it to: %s - error: %s" %
               (data_file.filename,
                data_file.filename.replace(LIVE, BROKEN),
                IOError("I'm a broken file")))
        self.assertTrue(self.memento.check_warning(msg))

    def test_build_keydir_on_init(self):
        """Test _build_keydir method."""
        db = Tritcask(self.base_dir)
        # create some stuff.
        for i in range(10):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        db.shutdown()
        old_keydir = db._keydir
        db = Tritcask(self.base_dir)
        self.addCleanup(db.shutdown)
        # check that the keydir is the same.
        self.assertEqual(old_keydir, db._keydir)

    def test_build_keydir_with_bad_data(self):
        """Test _build_keydir method with a bad data file."""
        db = Tritcask(self.base_dir)
        # create some stuff.
        for i in range(10):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        live_filename = db.live_file.filename
        db.shutdown()
        with open(live_filename, 'rb+') as fd:
            # seek to the middle of the last bytes
            fd.seek(-5, os.SEEK_END)
            fd.write(os.urandom(len('this is bad data.')))
            fd.flush()
        old_keydir = db._keydir
        db = Tritcask(self.base_dir)
        self.addCleanup(db.shutdown)
        # check that the keydir is the expected:
        # the new keydir should have entries from 0-8
        # remove the extra entry from the old keydir
        old_keydir.pop((9, b'foo9'))
        self.assertEqual(old_keydir, db._keydir)

    def test_build_keydir_with_hint(self):
        """Test _build_keydir using a hint file."""
        db = Tritcask(self.base_dir)
        # create some stuff.
        for i in range(10):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        # make the data file inactive and generate the hint.
        db.live_file.make_immutable()
        old_keydir = db._keydir
        db.shutdown()
        Tritcask(self.base_dir).shutdown()
        # now create a new DB using the hint
        db = Tritcask(self.base_dir)
        self.addCleanup(db.shutdown)
        self.assertEqual(old_keydir, db._keydir)

    def test_build_keydir_with_multiple_hint(self):
        """Test _build_keydir using a several hint files."""
        db = Tritcask(self.base_dir)
        # create some stuff.
        for i in range(10):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        # make the data file inactive and generate the hint.
        db.live_file.make_immutable()
        db.shutdown()
        # create more stuff.
        db = Tritcask(self.base_dir)
        for i in range(20, 30):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        # make the data file inactive and generate the hint.
        db.live_file.make_immutable()
        old_keydir = db._keydir
        db.shutdown()
        Tritcask(self.base_dir).shutdown()
        # now create a new DB using the hint
        db = Tritcask(self.base_dir)
        self.addCleanup(db.shutdown)
        self.assertEqual(old_keydir, db._keydir)

    def test_build_keydir_build_hint(self):
        """Test _build_keydir build the hints"""
        db = Tritcask(self.base_dir)
        # create some stuff.
        for i in range(10):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        # make the data file inactive and generate the hint.
        hint_filename = db.live_file.make_immutable().hint_filename
        db.shutdown()
        db = Tritcask(self.base_dir)
        self.addCleanup(db.shutdown)
        self.assertTrue(os.path.exists(hint_filename))
        # check that the hint matches the contents in the DB
        hint_file = HintFile(hint_filename)
        for hint_entry in hint_file.iter_entries():
            self.assertEqual(6, len(hint_entry))
            self.assertTrue(db.get(hint_entry[2], hint_entry[-1]) is not None)

    def test_build_keydir_build_hint_only_for_immutable(self):
        """Test that _build_keydir build the hint only for immutable files."""
        db = Tritcask(self.base_dir)
        # create some stuff.
        for i in range(10):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        # make the data file inactive and generate the hint.
        db.rotate()
        hint_filename = list(db._immutable.values())[0].hint_filename
        # create some stuff.
        for i in range(20, 30):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        db.shutdown()
        db = Tritcask(self.base_dir)
        self.addCleanup(db.shutdown)
        self.assertFalse(os.path.exists(db.live_file.hint_filename))
        self.assertTrue(os.path.exists(hint_filename), os.listdir(db.base_path))
        # check that the hint matches the contents in the DB
        hint_file = HintFile(hint_filename)
        for hint_entry in hint_file.iter_entries():
            self.assertEqual(6, len(hint_entry))
            self.assertTrue(db.get(hint_entry[2], hint_entry[-1]) is not None)

    def test_build_keydir_with_empty_hint(self):
        """Test _build_keydir using a hint file."""
        db = Tritcask(self.base_dir)
        # create some stuff.
        for i in range(10):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        # make the data file inactive and generate the hint.
        db.live_file.make_immutable()
        old_keydir = db._keydir
        db.shutdown()
        db = Tritcask(self.base_dir)
        hint_filename = list(db._immutable.values())[0].hint_filename
        # truncate the hint file
        open(hint_filename, 'w').close()
        db.shutdown()
        # open the tritcask and make sure it regenerates the hint file
        db = Tritcask(self.base_dir)
        self.addCleanup(db.shutdown)
        self.assertTrue(os.path.exists(hint_filename), os.listdir(db.base_path))
        self.assertEqual(old_keydir, db._keydir)

    def test_build_keydir(self):
        """Test _build_keydir method."""
        db = Tritcask(self.base_dir)
        # create some stuff.
        for i in range(10):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        old_keydir = db._keydir
        # shutdown (close the files)
        db.shutdown()
        db = Tritcask(self.base_dir)
        self.addCleanup(db.shutdown)
        self.assertEqual(old_keydir, db._keydir)

    def test_build_keydir_with_dead_rows(self):
        """Test _build_keydir method with TOMBSTONE rows."""
        db = Tritcask(self.base_dir)
        # create some stuff.
        for i in range(10):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        # delete half the nodes
        for i in range(0, 10, 2):
            db.delete(i, ('foo%d' % (i,)).encode('ascii'))
        for i in range(10, 20):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        old_keydir = db._keydir
        # shutdown (close the files)
        db.shutdown()
        db = Tritcask(self.base_dir)
        self.addCleanup(db.shutdown)
        self.assertEqual(old_keydir, db._keydir)

    def test_build_keydir_build_hint_with_dead_rows(self):
        """Test that _build_keydir build the hint with TOMBSTONE rows."""
        db = Tritcask(self.base_dir, dead_bytes_threshold=0.1)
        # create some stuff.
        for i in range(100):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        # create a inactive file with all the items
        db.rotate()
        db.shutdown()
        # delete half of the items
        db = Tritcask(self.base_dir, dead_bytes_threshold=0.01)
        for i in range(50):
            db.delete(i, ('foo%d' % (i,)).encode('ascii'))
        # add one item so the hint file have at leats one
        db.put(100, b'foo100', b'bar100')
        assert len(db._keydir.keys()) == 51, len(db._keydir.keys())
        old_keydir = db._keydir
        db.shutdown()
        # trigger a rotate and a hint build
        db = Tritcask(self.base_dir, dead_bytes_threshold=0.01)
        db.rotate()
        db.shutdown()
        # trigger a hint build of the last rotated file
        Tritcask(self.base_dir, dead_bytes_threshold=0.01).shutdown()
        db = Tritcask(self.base_dir, dead_bytes_threshold=0.01)
        self.addCleanup(db.shutdown)
        self.assertEqual(sorted(old_keydir.keys()), sorted(db._keydir.keys()))

    def test_should_rotate(self):
        """Test should_rotate method."""
        db = Tritcask(self.base_dir)
        self.addCleanup(db.shutdown)
        # add some data
        for i in range(10):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        self.assertFalse(db.should_rotate())
        # ovewrite it 4 times
        for i in range(4):
            for i in range(10):
                db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        self.assertTrue(db.should_rotate())

    def test_should_merge(self):
        """Test should_merge method."""
        db = Tritcask(self.base_dir)
        self.addCleanup(db.shutdown)
        # add some data
        for i in range(10):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        db.rotate()
        self.assertFalse(db.should_merge(db._immutable))
        # overite it 4 times
        for i in range(4):
            for i in range(10):
                db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
            db.rotate()
        self.assertTrue(db.should_merge(db._immutable))

    def test_should_merge_max_files(self):
        """Test should_merge method based on max_im files ."""
        db = Tritcask(self.base_dir, max_immutable_files=10)
        self.addCleanup(db.shutdown)
        # add some data
        # create 10 immutable files
        for j in range(10):
            for i in range(2):
                db.put(i * j, ('foo%d' % (i * j + i,)).encode('ascii'),
                       ('bar%s' % (i,)).encode('ascii'))
            db.rotate()
        self.assertFalse(db.should_merge(db._immutable))
        # create the 11th immutable file
        db.put(200, b'foo200', b'bar200')
        db.rotate()
        self.assertTrue(db.should_merge(db._immutable))

    def test_should_merge_no_stats(self):
        """Test should_merge method without stats for a data file."""
        db = Tritcask(self.base_dir, max_immutable_files=10)
        # add some data
        for j in range(10):
            db.put(j, ('foo%d' % (j,)).encode('ascii'), ('bar%s' % (j,)).encode('ascii'))
        # rotate the file to create a immutable with all the live rows
        db.rotate()
        # delete everything
        for j in range(10):
            db.delete(j, ('foo%d' % (j,)).encode('ascii'))
        # rotate the file to create an immutable with all the tombstones
        fid = db.live_file.file_id
        db.rotate()
        db.shutdown()
        # start with auto_merge=False
        db = Tritcask(self.base_dir, auto_merge=False)
        self.addCleanup(db.shutdown)
        # check that we don't have any stats for the file with the tombstones
        self.assertRaises(KeyError, db._keydir.get_stats, fid)
        # check the should_merge works as expected
        self.assertTrue(db.should_merge(db._immutable))

    def test__rotate_and_not_merge(self):
        """Test _rotate_and_merge method."""
        db = Tritcask(self.base_dir)
        # add the same data in 5 different data files
        for i in range(20):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        db.rotate()
        for i in range(5):
            for i in range(10):
                db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        called = []
        db.shutdown()
        self.patch(Tritcask, 'rotate', lambda *a, **k: called.append('rotate'))
        self.patch(Tritcask, 'merge', lambda *args: called.append('merge'))
        Tritcask(self.base_dir).shutdown()
        self.assertIn('rotate', called)
        self.assertNotIn('merge', called)

    def test__rotate_and_merge(self):
        """Test _rotate_and_merge method."""
        db = Tritcask(self.base_dir)
        # add the slightly different data in 5 different data files
        for j in range(5):
            for i in range(20):
                db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
            db.rotate()
        for j in range(5):
            for i in range(15):
                db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        called = []
        db.shutdown()
        self.patch(Tritcask, 'rotate', lambda *a, **k: called.append('rotate'))
        self.patch(Tritcask, 'merge', lambda *args: called.append('merge'))
        Tritcask(self.base_dir).shutdown()
        self.assertIn('rotate', called)
        self.assertIn('merge', called)

    def test_rotate_on_bad_crc(self):
        """Test that the live file is rotated when a BadCrc is found."""
        db = Tritcask(self.base_dir)
        # add some data
        for i in range(10):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        self.assertFalse(db.should_rotate())
        # write a different value -> random bytes
        # now write some garbage to the end of file
        db.live_file.fd.write(os.urandom(100))
        db.live_file.fd.flush()
        db.shutdown()
        called = []
        self.patch(Tritcask, 'rotate', lambda *a, **k: called.append('rotate'))
        Tritcask(self.base_dir).shutdown()
        self.assertIn('rotate', called)


class BasicTest(BaseTestCase):

    def setUp(self):
        super(BasicTest, self).setUp()
        self.db = Tritcask(self.base_dir)
        self.addCleanup(self.db.shutdown)

    def test_put(self):
        """Basic test for the put method."""
        key, data = self.build_data()
        self.db.put(0, key, data)
        self.assertEqual(1, len(self.db._keydir.keys()))
        # check that the entry is in the file
        with open(self.db.live_file.filename, 'r+b') as f:
            raw_data_len = (len(key) + len(data) + crc32_size + header_size)
            raw_data = f.read(raw_data_len)
            header = header_struct.unpack(
                raw_data[crc32_size:crc32_size + header_size])
            tstamp, key_sz, value_sz, row_type = header
            self.assertEqual(data, raw_data[crc32_size + header_size + key_sz:])
        self.assertEqual(self.db.get(0, key), data)
        # add new entry and check
        key_1, data_1 = self.build_data()
        self.db.put(1, key_1, data_1)
        self.assertEqual(2, len(self.db._keydir.keys()))
        self.assertEqual(self.db.get(0, key), data)
        self.assertEqual(self.db.get(1, key_1), data_1)

    def test_delete(self):
        """Basic test for the delete method."""
        key, data = self.build_data()
        self.db.put(0, key, data)
        self.db.delete(0, key)
        # check that the TOMBSTONE is there for these keys
        with open(self.db.live_file.filename, 'r+b') as f:
            raw_data_len = len(key) + len(TOMBSTONE) + crc32_size + header_size
            f.seek(-1 * raw_data_len, os.SEEK_END)
            raw_data = f.read(raw_data_len)
            self.assertEqual(TOMBSTONE, raw_data[crc32_size + header_size + len(key):])

    def test_delete_stats_updated(self):
        """Test that calling delete update the keydir stats."""
        key, data = self.build_data()
        self.db.put(0, key, data)
        stats = self.db._keydir.get_stats(self.db.live_file.file_id)
        self.assertEqual(1, stats['live_entries'])
        self.db.delete(0, key)
        stats = self.db._keydir.get_stats(self.db.live_file.file_id)
        self.assertEqual(0, stats['live_entries'])
        self.assertEqual(0, stats['live_bytes'])

    def test_get(self):
        """Basic test for the get method."""
        key, data = self.build_data()
        self.db.put(0, key, data)
        self.assertEqual(self.db.get(0, key), data)
        self.db.delete(0, key)

    def test_put_only_bytes_key(self):
        """Test that put only works with bytes keys."""
        _, data = self.build_data()
        self.assertRaises(ValueError, self.db.put, 0, None, data)
        self.assertRaises(ValueError, self.db.put, 0, u'foo', data)
        self.assertRaises(ValueError, self.db.put, 0, object(), data)

    def test_put_only_bytes_value(self):
        """Test that put only works with bytes keys."""
        key, _ = self.build_data()
        self.assertRaises(ValueError, self.db.put, 0, key, None)
        self.assertRaises(ValueError, self.db.put, 0, key, u'foo')
        self.assertRaises(ValueError, self.db.put, 0, key, object())

    def test_get_only_bytes(self):
        """Test that get only works with bytes."""
        key, data = self.build_data()
        self.db.put(0, key, data)
        self.assertRaises(ValueError, self.db.get, 0, None)
        self.assertRaises(ValueError, self.db.get, 0, u'foobar')
        self.assertRaises(ValueError, self.db.get, 0, object())

    def test_delete_only_bytes(self):
        """Test that delete only works with bytes."""
        key, data = self.build_data()
        self.assertRaises(ValueError, self.db.delete, 0, None)
        self.assertRaises(ValueError, self.db.delete, 0, u'foobar')
        self.assertRaises(ValueError, self.db.delete, 0, object())

    def test_keys(self):
        """Test for the keys() method."""
        # add some values
        key, data = self.build_data()
        self.db.put(0, key, data)
        key, data = self.build_data()
        self.db.put(0, key, data)
        self.assertEqual(self.db._keydir.keys(), self.db.keys())

    def test__contains__(self):
        """Test for __contains__ method."""
        key, data = self.build_data()
        self.db.put(0, key, data)
        key1, data1 = self.build_data()
        self.db.put(0, key1, data1)
        self.assertTrue((0, key) in self.db._keydir)
        self.assertTrue((0, key) in self.db)
        self.assertTrue((0, key1) in self.db._keydir)
        self.assertTrue((0, key1) in self.db)


class MergeTests(BaseTestCase):

    def _add_data(self, db, size=100):
        """Add random data to a db."""
        for i in range(size):
            key, value = self.build_data()
            db.put(0, key, value)

    def test_simple_merge(self):
        """Test a simple merge."""
        # create 3 immutable files
        db = Tritcask(self.base_dir)
        self._add_data(db, 100)
        db.rotate()
        self._add_data(db, 100)
        db.rotate()
        self._add_data(db, 100)
        db.shutdown()
        db = Tritcask(self.base_dir)
        self.addCleanup(db.shutdown)
        keydir_to_merge = db._keydir.copy()
        # add data to the live file
        self._add_data(db, 100)
        immutable_fnames = [ifile.filename for ifile in db._immutable.values()]
        # do the merge.
        data_file = db.merge(db._immutable)
        for entry in data_file.iter_entries():
            (crc32, tstamp, key_sz, value_sz, row_type,
                key, value, value_pos) = entry
            keydir_entry = keydir_to_merge[(row_type, key)]
            (old_file_id, old_tstamp, old_value_sz, old_value_pos) = keydir_entry
            old_value = db._get_value(old_file_id, old_value_pos, old_value_sz)
            self.assertEqual(old_value, value)
        for fname in immutable_fnames:
            self.assertFalse(os.path.exists(fname))
            self.assertTrue(os.path.exists(fname.replace(INACTIVE, DEAD)))

    def test_single_file_merge(self):
        """Test a single file merge (a.k.a compactation)."""
        # create 3 immutable files
        db = Tritcask(self.base_dir)
        self._add_data(db, 100)
        db.rotate()
        self._add_data(db, 100)
        db.rotate()
        self._add_data(db, 100)
        db.shutdown()
        db = Tritcask(self.base_dir)
        self.addCleanup(db.shutdown)
        keydir_to_merge = db._keydir.copy()
        # add data to the live file
        self._add_data(db, 100)
        immutable_fnames = [ifile.filename for ifile in db._immutable.values()]
        # do the merge.
        data_file = db.merge(db._immutable)
        for entry in data_file.iter_entries():
            (crc32, tstamp, key_sz, value_sz, row_type,
                key, value, value_pos) = entry
            keydir_entry = keydir_to_merge[(row_type, key)]
            (old_file_id, old_tstamp, old_value_sz, old_value_pos) = keydir_entry
            old_value = db._get_value(old_file_id, old_value_pos, old_value_sz)
            self.assertEqual(old_value, value)
        for fname in immutable_fnames:
            self.assertFalse(os.path.exists(fname))
            self.assertTrue(os.path.exists(fname.replace(INACTIVE, DEAD)))

    def test_single_auto_merge(self):
        """Test auto-merge in startup.

        Starting with a very fragmented file triggers a rotation and merge.
        """
        db = Tritcask(self.base_dir)
        self._add_data(db, 100)
        # delete almost all entrie to trigger a merge
        for i, k in enumerate(list(db._keydir.keys())):
            if i <= 90:
                db.delete(*k)
        old_live_file = db.live_file
        db.shutdown()
        # at this moment we only have the live file
        # start a new Tritcask instance.
        db = Tritcask(self.base_dir)
        self.addCleanup(db.shutdown)
        self.assertFalse(old_live_file.exists())
        self.assertIn(old_live_file.file_id, db._immutable)

    def test_auto_merge(self):
        """Test auto-merge in startup.

        Starting with fragmented files triggers a rotation and merge.
        """
        db = Tritcask(self.base_dir)
        self._add_data(db, 100)
        # delete almost all entrie to trigger a merge
        for i, k in enumerate(list(db._keydir.keys())):
            if i <= 90:
                db.delete(*k)
        old_live_file = db.live_file
        db.rotate()
        self._add_data(db, 100)
        # delete almost all entrie to trigger a merge
        for i, k in enumerate(list(db._keydir.keys())):
            if i <= 90:
                db.delete(*k)
        old_live_file_1 = db.live_file
        db.shutdown()
        # at this moment we only have the live file
        # start a new Tritcask instance.
        db = Tritcask(self.base_dir)
        self.addCleanup(db.shutdown)
        self.assertFalse(old_live_file.exists())
        self.assertFalse(old_live_file_1.exists())
        self.assertIn(old_live_file.file_id, db._immutable)
        self.assertIn(old_live_file_1.file_id, db._immutable)

    def test_merge_do_nothing_all_dead_entries_single_file(self):
        """Test possible merge of a immutable file with 100% dead data.

        The expected side effect is that the file should be marked as immutable,
        then merged and marked as dead. And on the next startup deleted.
        """
        db = Tritcask(self.base_dir)
        self._add_data(db, 100)
        # delete almost all entrie to trigger a merge
        for i, k in enumerate(list(db._keydir.keys())):
            if i < 100:
                db.delete(*k)
        db.shutdown()
        # start tritcask without automerge and check
        db = Tritcask(self.base_dir, auto_merge=False)
        files = sorted(os.listdir(db.base_path))
        try:
            self.assertEqual(2, len(files))
            self.assertIn(INACTIVE, files[0])
            self.assertIn(LIVE, files[1])
        finally:
            db.shutdown()
        # trigger the rotation
        # now we should have a single immutable_file (previous live one)
        db = Tritcask(self.base_dir)
        try:
            self.assertEqual(1, len(db._immutable))
            files = sorted(os.listdir(db.base_path))
            self.assertEqual(3, len(files))
            self.assertIn(DEAD, files[0])
            self.assertIn(DEAD, files[1])
            self.assertIn(LIVE, files[2])
        finally:
            db.shutdown()
        # at this moment we have the live + imm_file files
        # start a new Tritcask instance.
        db = Tritcask(self.base_dir)
        try:
            files = sorted(os.path.join(self.base_dir, f) for f in os.listdir(db.base_path))
            # self.assertIn(imm_file.filename.replace(INACTIVE, DEAD), files)
            # the immutable_file should be dead as there are no live entries
            # there should be only 2 files, the dead and the live
            # dead + dead_hint + live
            files = sorted(os.listdir(db.base_path))
            self.assertEqual(1, len(files), files)
            # self.assertIn(DEAD, files[0])
            # self.assertIn(DEAD, files[1])
            # self.assertIn(HINT, files[1])
            self.assertIn(LIVE, files[0])
        finally:
            db.shutdown()
        # start a new Tritcask to check everything is ok after the
        # merge.
        Tritcask(self.base_dir).shutdown()
        # check that the dead file is no more
        files = sorted(os.listdir(db.base_path))
        self.assertEqual(1, len(files), files)

    def test_merge_mixed_dead_entries(self):
        """Test merge of 2 immutable files one with 100%% dead data.

        The expected side effect is that the file should be marked as dead.
        """
        db = Tritcask(self.base_dir)
        self._add_data(db, 100)
        # delete almost all entrie to trigger a merge
        for i, k in enumerate(list(db._keydir.keys())):
            if i <= 100:
                db.delete(*k)
        db.shutdown()
        db = Tritcask(self.base_dir, auto_merge=False)
        try:
            self.assertEqual(1, len(db._immutable))
            imm_file_1 = list(db._immutable.values())[0]
        finally:
            db.shutdown()
        db = Tritcask(self.base_dir)
        # the "empty" immutable_file should be dead as there are no live entries
        # but it's also removed from the _immutable dict
        files = sorted(os.path.join(self.base_dir, f) for f in os.listdir(db.base_path))
        try:
            self.assertIn(imm_file_1.filename.replace(INACTIVE, DEAD), files)
            self._add_data(db, 100)
            for i, k in enumerate(list(db._keydir.keys())):
                if i % 2:
                    db.delete(*k)
        finally:
            db.shutdown()
        # at this moment we only have the live file
        # start a new Tritcask instance.
        db = Tritcask(self.base_dir)
        # there should be 5 files:
        #    - the immutable file just merged as a DEAD file.
        #    - the new immutable_file
        #    - the current live file
        files = sorted(os.listdir(db.base_path))
        try:
            self.assertEqual(4, len(files), files)
            self.assertIn(DEAD, files[0])
            self.assertIn(INACTIVE, files[1])
            self.assertIn(INACTIVE, files[2])
            self.assertIn(HINT, files[2])
            self.assertIn(LIVE, files[3])
        finally:
            db.shutdown()
        # start a new Tritcask to check everything is ok after the
        # merge.
        Tritcask(self.base_dir).shutdown()
        # check that the dead files were deleted
        files = sorted(os.listdir(db.base_path))
        self.assertEqual(3, len(files), files)
        self.assertIn(INACTIVE, files[0])
        self.assertIn(INACTIVE, files[1])
        self.assertIn(HINT, files[1])
        self.assertIn(LIVE, files[2])

    def test_merged_file_is_older_than_live(self):
        """Test that merge creates and live file newer than the merge result.

        The expected side effect is that the live file should be newer than the
        merged immutable file.
        """
        db = Tritcask(self.base_dir)
        self._add_data(db, 100)
        # delete almost all entrie to trigger a merge
        for i, k in enumerate(list(db._keydir.keys())):
            if i <= 60:
                db.delete(*k)
        db.shutdown()
        # trigger the rotation
        # now we should have a single immutable_file (previous live one)
        db = Tritcask(self.base_dir)
        try:
            self.assertEqual(1, len(db._immutable))
            dead_file = list(db._immutable.values())[0]
            self.assertIn(DEAD, dead_file.filename)
            files = sorted(os.listdir(db.base_path))
            self.assertEqual(4, len(files))
            self.assertIn(DEAD, files[0])
            self.assertIn(INACTIVE, files[1])
            self.assertIn(HINT, files[2])
            self.assertIn(LIVE, files[3])
            self.assertEqual(db.live_file.size, 0)
        finally:
            db.shutdown()
        # at this moment we have the live + imm_file files
        # start a new Tritcask instance.
        db = Tritcask(self.base_dir)
        self.addCleanup(db.shutdown)
        # the dead file should be fone
        # there should be only 2 files, the immutable and the live
        # immutable + hint + live
        files = sorted(os.listdir(db.base_path))
        self.assertEqual(3, len(files), files)
        self.assertIn(INACTIVE, files[0])
        self.assertIn(HINT, files[1])
        self.assertIn(LIVE, files[2])
        assert len(db._immutable) == 1, "More than 1 immutable file."
        live_id = int(db.live_file.file_id)
        imm_id = int(list(db._immutable.keys())[0])
        self.assertTrue(live_id > imm_id, "%d <= %d" % (live_id, imm_id))


class KeydirStatsTests(BaseTestCase):
    """Tests for the Keydir stats handling."""

    def test_setitem(self):
        """Test that __setitem__ correctly update the stats."""
        keydir = Keydir()
        file_id = DataFile._get_next_file_id()
        for i in range(10):
            keydir[(0, str(uuid.uuid4()))] = KeydirEntry(
                file_id, timestamp(), len(str(uuid.uuid4())), i + 10)
        file_id_1 = DataFile._get_next_file_id()
        for i in range(20):
            keydir[(0, str(uuid.uuid4()))] = KeydirEntry(
                file_id_1, timestamp(), len(str(uuid.uuid4())), i + 10)
        entry_size = len(str(uuid.uuid4())) * 2 + header_size + crc32_size
        self.assertEqual(entry_size * 10, keydir._stats[file_id]['live_bytes'])
        self.assertEqual(entry_size * 20, keydir._stats[file_id_1]['live_bytes'])

    def test_update_entry(self):
        """Test that __setitem__ updates the stats for an entry."""
        keydir = Keydir()
        file_id = DataFile._get_next_file_id()
        key = str(uuid.uuid4())
        entry = KeydirEntry(file_id, timestamp(), 1, 1)
        keydir[(0, key)] = entry
        base_size = len(key) + header_size + crc32_size
        self.assertEqual(keydir._stats[file_id]['live_bytes'], base_size + 1)
        new_entry = KeydirEntry(file_id, timestamp(), 2, 2)
        keydir[(0, key)] = new_entry
        self.assertEqual(keydir._stats[file_id]['live_bytes'], base_size + 2)

    def test_update_entry_different_file_id(self):
        """Test that __setitem__ updates the stats for an entry.

        Even if the entry was in a different file id.
        """
        keydir = Keydir()
        file_id = DataFile._get_next_file_id()
        key = str(uuid.uuid4())
        entry = KeydirEntry(file_id, timestamp(), 10, 1)
        keydir[(0, key)] = entry
        base_size = len(key) + header_size + crc32_size
        self.assertEqual(keydir._stats[file_id]['live_bytes'], base_size + 10)
        new_file_id = DataFile._get_next_file_id()
        new_entry = KeydirEntry(new_file_id, timestamp(), 5, 10)
        keydir[(0, key)] = new_entry
        self.assertEqual(keydir._stats[new_file_id]['live_bytes'],
                         base_size + 5)
        self.assertEqual(keydir._stats[file_id]['live_bytes'], 0)

    def test_remove(self):
        """Test that remove correctly update the stats."""
        keydir = Keydir()
        file_id = DataFile._get_next_file_id()
        for i in range(10):
            key = str(uuid.uuid4())
            keydir[(0, key)] = KeydirEntry(file_id, timestamp(),
                                           len(str(uuid.uuid4())), i + 10)
            if i % 2:
                keydir.remove((0, key))
        file_id_1 = DataFile._get_next_file_id()
        for i in range(20):
            key = str(uuid.uuid4())
            keydir[(0, key)] = KeydirEntry(file_id_1, timestamp(),
                                           len(str(uuid.uuid4())), i + 10)
            if i % 2:
                keydir.remove((0, key))
        entry_size = len(str(uuid.uuid4())) * 2 + header_size + crc32_size
        self.assertEqual(entry_size * (10 / 2),
                         keydir._stats[file_id]['live_bytes'])
        self.assertEqual(entry_size * (20 / 2),
                         keydir._stats[file_id_1]['live_bytes'])

    def test_remove_missing_key(self):
        """Test the remove method with a missing key."""
        keydir = Keydir()
        key = str(uuid.uuid4())
        try:
            keydir.remove((0, key))
        except KeyError as e:
            self.fail(e)

    def test_get_stats(self):
        """Test the get_stats method."""
        keydir = Keydir()
        file_id = DataFile._get_next_file_id()
        for i in range(10):
            keydir[(0, str(uuid.uuid4()))] = KeydirEntry(
                file_id, timestamp(), len(str(uuid.uuid4())), i + 10)
        self.assertEqual(keydir._stats[file_id], keydir.get_stats(file_id))
        self.assertTrue(keydir._stats[file_id] is not keydir.get_stats(file_id))

    def test_get_stats_missing(self):
        """Test get_stats with a missing file_id."""
        keydir = Keydir()
        file_id = DataFile._get_next_file_id()
        self.assertRaises(KeyError, keydir.get_stats, file_id)


class TritcaskShelfTests(BaseTestCase):
    """Tests for TritcaskShelf."""

    def test_invalid_keys(self):
        """Test the exception raised when invalid keys are used ('', None)."""
        path = os.path.join(self.base_dir, 'shelf_invalid_keys')
        shelf = TritcaskShelf(0, Tritcask(path))
        self.addCleanup(shelf._db.shutdown)
        self.assertRaises(ValueError, shelf.__setitem__, None, 'foo')
        self.assertRaises(ValueError, shelf.__setitem__, '', 'foo')

    def test_contains(self):
        """Test that it behaves with the 'in'."""
        path = os.path.join(self.base_dir, 'shelf_contains')
        shelf = TritcaskShelf(0, Tritcask(path))
        self.addCleanup(shelf._db.shutdown)
        shelf[b"foo"] = b"bar"
        self.assertTrue(b"foo" in shelf)
        self.assertFalse(b"baz" in shelf)
        self.assertEqual(b'bar', shelf.get(b'foo'))
        self.assertEqual(None, shelf.get(b'baz', None))

    def test_pop(self):
        """Test that .pop() works."""
        path = os.path.join(self.base_dir, 'shelf_pop')
        shelf = TritcaskShelf(0, Tritcask(path))
        self.addCleanup(shelf._db.shutdown)
        shelf[b"foo"] = b"bar"
        self.assertEqual(b"bar", shelf.pop(b"foo"))
        self.assertFalse(b"foo" in shelf)
        # bad key
        self.assertRaises(KeyError, shelf.pop, b"no-key")

    def test_get(self):
        """Test that it behaves with the .get(key, default)."""
        path = os.path.join(self.base_dir, 'shelf_get')
        shelf = TritcaskShelf(0, Tritcask(path))
        self.addCleanup(shelf._db.shutdown)
        shelf[b"foo"] = b"bar"
        self.assertEqual(b'bar', shelf.get(b'foo'))
        self.assertEqual(b'bar', shelf.get(b'foo', None))
        self.assertEqual(None, shelf.get(b'baz'))
        self.assertFalse(shelf.get(b'baz', False))

    def test_items(self):
        """Test the items method."""
        path = os.path.join(self.base_dir, 'shelf_items')
        shelf = TritcaskShelf(0, Tritcask(path))
        self.addCleanup(shelf._db.shutdown)
        shelf[b"foo"] = b"bar"
        self.assertEqual([(b'foo', b'bar')], list(shelf.items()))
        shelf[b"foo1"] = b"bar1"
        self.assertIn((b'foo', b'bar'), shelf.items())
        self.assertIn((b'foo1', b'bar1'), shelf.items())

    def test_custom_serialization(self):
        """Test the _serialize and _deserialize methods."""
        path = os.path.join(self.base_dir, 'shelf_serialization')

        class MarshalShelf(TritcaskShelf):
            """A shelf that use marshal for de/serialization."""

            def _deserialize(self, value):
                """Custom _serialize."""
                return marshal.loads(value)

            def _serialize(self, value):
                """Custom _deserialize."""
                return marshal.dumps(value)

        db = Tritcask(path)
        self.addCleanup(db.shutdown)
        shelf = MarshalShelf(0, db)
        shelf[b'foo'] = b'bar'
        self.assertIn(b'bar', shelf[b'foo'])
        self.assertEqual(db.get(0, b'foo'), marshal.dumps(b'bar'))

    def test_keys(self):
        """Test for the keys method."""
        path = os.path.join(self.base_dir, 'shelf_get')
        shelf = TritcaskShelf(0, Tritcask(path))
        self.addCleanup(shelf._db.shutdown)
        for i in range(10):
            shelf[('foo%d' % (i,)).encode('ascii')] = ('bar%d' % (i,)).encode('ascii')
        keys = shelf.keys()
        self.assertEqual(types.GeneratorType, type(keys))
        self.assertEqual(10, len(list(keys)))

    def test__len__(self):
        """Test for the __len__ method."""
        path = os.path.join(self.base_dir, 'shelf_get')
        shelf = TritcaskShelf(0, Tritcask(path))
        self.addCleanup(shelf._db.shutdown)
        for i in range(10):
            shelf[('foo%d' % (i,)).encode('ascii')] = ('bar%d' % (i,)).encode('ascii')
        self.assertEqual(10, len(shelf))
        shelf[b'foo_a'] = b'bar_a'
        self.assertEqual(11, len(shelf))


class WindowsTimerTests(TestCase):
    """Tests for the windows timer."""

    def test_initial_value(self):
        """Test that the initial value is > 0."""
        timer = WindowsTimer()
        self.assertTrue(int(timer.time()) > 0)
