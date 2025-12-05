# tests.test_tritcask_pytest - tritcask tests (pytest version)
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
"""Tests for Tritcask and helper functions (pytest version)."""

import contextlib
import logging
import marshal
import mmap
import os
import time
import types
import uuid

import pytest

import tritcask
from tritcask import logger, DataFile
import zlib

from operator import attrgetter

from tritcask.tritcask import (
    timestamp,
    Tritcask,
    header_size,
    crc32_size,
    crc32_struct,
    header_struct,
    BadCrc,
    BadHeader,
    TempDataFile,
    DeadDataFile,
    ImmutableDataFile,
    HintFile,
    HintEntry,
    hint_header_size,
    hint_header_struct,
    _get_file_id,
    is_live,
    is_immutable,
    is_hint,
    LIVE,
    HINT,
    INACTIVE,
    DEAD,
    BROKEN,
    TOMBSTONE,
    Keydir,
    KeydirEntry,
    TritcaskShelf,
    WindowsTimer,
    Cask,
)
from contrib.handlers import MementoHandler


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def base_dir(tmp_path):
    """Create a base directory for tests."""
    data_dir = tmp_path / "data_dir"
    data_dir.mkdir()
    return str(data_dir)


@pytest.fixture
def root_dir(tmp_path):
    """Create a root directory for tests."""
    root = tmp_path / "root_dir"
    root.mkdir()
    return str(root)


@pytest.fixture
def memento():
    """Create and configure a MementoHandler for capturing log messages."""
    handler = MementoHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    yield handler
    logger.removeHandler(handler)


@pytest.fixture(autouse=True)
def reset_datafile_id():
    """Reset DataFile.last_generated_id after each test."""
    yield
    DataFile.last_generated_id = 0


def build_data():
    """Build a random key/value."""
    key = str(uuid.uuid4()).encode('ascii')
    data = os.urandom(50)
    return key, data


# ============================================================================
# DataFileTest
# ============================================================================

class TestDataFile:
    """Tests for DataFile class."""

    file_class = DataFile

    def test__get_next_file_id(self):
        """Test for _get_next_file_id method."""
        curr = int(timestamp() * 100000)
        assert curr <= int(self.file_class._get_next_file_id())
        curr = int(timestamp() * 100000)
        assert curr <= int(self.file_class._get_next_file_id())

    def test_lightning_fast_virtualbox_sometimes_lies(self, monkeypatch):
        """Win on Virtualbox sometimes returns 0 in time.process_time."""
        test_items = 100
        monkeypatch.setattr(time, "clock", lambda: 0, raising=False)
        monkeypatch.setattr(time, "process_time", lambda: 0)
        file_ids = [self.file_class._get_next_file_id()
                    for _ in range(test_items)]
        # even though the clock is not moving, all ids must be different!
        assert len(set(file_ids)) == test_items

    def test_log_warning_repeated_timestamps(self, monkeypatch, memento):
        """When the clock is not moving, we log a warning."""
        expected_warning = "Repeated timestamps"
        ts = timestamp() + 1.0
        monkeypatch.setattr(tritcask.tritcask, "timestamp", lambda: ts)

        id1 = self.file_class._get_next_file_id()
        assert not memento.check_warning(expected_warning)

        id2 = self.file_class._get_next_file_id()
        assert id1 != id2
        assert memento.check_warning(expected_warning)

    def test_exists(self, base_dir):
        """Tests for exists method."""
        new_file = self.file_class(base_dir)
        try:
            assert new_file.exists()
            assert os.path.exists(new_file.filename)
        finally:
            new_file.close()

    def test_size(self, base_dir):
        """Test the size property."""
        new_file = self.file_class(base_dir)
        try:
            assert new_file.size == 0
            new_file.fd.write(b'foo')
            new_file.fd.flush()
            assert new_file.size == len(b'foo')
        finally:
            new_file.close()

    def test_has_hint(self, base_dir):
        """Test that has_hint works as expected."""
        new_file = self.file_class(base_dir)
        try:
            assert not new_file.has_hint
        finally:
            new_file.close()

    def test_hint_size(self, base_dir):
        """Test that hint_size work as expected."""
        new_file = self.file_class(base_dir)
        try:
            assert new_file.hint_size == 0
        finally:
            new_file.close()

    def test__open(self, base_dir):
        """Test the _open private method."""
        new_file = self.file_class(base_dir)
        try:
            # check that the file is opened
            new_file.fd.write(b'foo')
        finally:
            new_file.close()

    def test_close(self, base_dir):
        """Test the close method."""
        new_file = self.file_class(base_dir)
        new_file.close()
        assert new_file.fd is None

    def test_make_immutable(self, base_dir):
        """Test for make_immutable method."""
        new_file = self.file_class(base_dir)
        try:
            new_file.fd.write(b'foo')
            new_file.fd.flush()
            immutable_file = new_file.make_immutable()
            try:
                # the DataFile should be closed
                assert new_file.fd is None
                assert immutable_file.file_id == new_file.file_id
            finally:
                immutable_file.close()
        finally:
            new_file.close()

    def test_iter_entries(self, base_dir):
        """Test for iter_entries"""
        db = Tritcask(base_dir)
        try:
            for i in range(10):
                db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
            for i, entry in enumerate(db.live_file.iter_entries()):
                assert entry[4] == i
                assert entry[5] == ('foo%d' % (i,)).encode('ascii')
                assert entry[6] == ('bar%d' % (i,)).encode('ascii')
        finally:
            db.shutdown()

    def test_iter_entries_bad_crc(self, base_dir, memento):
        """Test that BadCrc during iter_entries is the same as EOF."""
        db = Tritcask(base_dir)
        try:
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
                assert entry[4] == i
                assert entry[5] == ('foo%d' % (i,)).encode('ascii')
                assert entry[6] == ('bar%d' % (i,)).encode('ascii')
            assert len(entries) == 10
            assert memento.check_warning('Found BadCrc on')
            assert memento.check_warning('the rest of the file will be ignored.')
            assert db.live_file.has_bad_crc, 'has_bad_crc should be True.'
        finally:
            db.shutdown()

    def test_iter_entries_bad_header_unpack(self, base_dir, memento):
        """Test that unpack error during iter_entries is the same as EOF."""
        db = Tritcask(base_dir)
        try:
            for i in range(10):
                db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
            # truncate the file at the start of the header of the last value.
            curr_pos = db.live_file.fd.tell()
            db.live_file.fd.seek(curr_pos - header_size + 4)
            db.live_file.fd.truncate()
            entries = []
            for i, entry in enumerate(db.live_file.iter_entries()):
                entries.append(entry)
                assert entry[4] == i
                assert entry[5] == ('foo%d' % (i,)).encode('ascii')
                assert entry[6] == ('bar%d' % (i,)).encode('ascii')
            assert len(entries) == 9
            assert db.live_file.has_bad_data, 'has_bad_data should be True.'
            assert memento.check_warning('Found corrupted header on')
            assert memento.check_warning('the rest of the file will be ignored.')
        finally:
            db.shutdown()

    def test__getitem__(self, base_dir):
        """Test our slicing support."""
        data_file = self.file_class(base_dir)
        try:
            tstamp, value_pos, value_sz = data_file.write(0, b'foo', b'bar')
            tstamp1, value1_pos, value1_sz = data_file.write(1, b'foo1', b'bar1')
            assert data_file[value_pos:value_pos + value_sz] == b'bar'
            assert data_file[value1_pos:value1_pos + value1_sz] == b'bar1'
        finally:
            data_file.close()

    def test__getitem__no_slice(self, base_dir):
        """Test that we *only* support slicing."""
        data_file = self.file_class(base_dir)
        try:
            tstamp, value_pos, value_sz = data_file.write(0, b'foo', b'bar')
            with pytest.raises(ValueError):
                data_file.__getitem__(value_pos)
        finally:
            data_file.close()

    def test_write(self, base_dir):
        """Test for write method."""
        data_file = self.file_class(base_dir)
        tstamp_1, value_1_pos, value_1_sz = data_file.write(0, b'foo', b'bar')
        assert value_1_sz == len(b'bar')
        tstamp_2, value_2_pos, value_2_sz = data_file.write(1, b'foo1', b'bar1')
        assert value_2_sz == len(b'bar1')
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
            assert zlib.crc32(raw_header + b'foo' + b'bar') & 0xFFFFFFFF == crc32
            assert len(b'foo') == key_sz
            assert len(b'bar') == value_sz
            assert b'bar' == value
            assert 0 == row_type

    def test_read(self, base_dir):
        """Test for read method."""
        data_file = self.file_class(base_dir)
        try:
            orig_tstamp, _, _ = data_file.write(0, b'foo', b'bar')
            tstamp1, _, _ = data_file.write(1, b'foo1', b'bar1')
            fmap = mmap.mmap(data_file.fd.fileno(), 0, access=mmap.ACCESS_READ)
            with contextlib.closing(fmap):
                current_pos = 0
                file_data, new_pos = data_file.read(fmap, current_pos)
                crc32, tstamp, key_sz, value_sz, row_type, key, value, pos = file_data
                current_pos = new_pos
                assert crc32_size + header_size + key_sz + value_sz == new_pos
                assert orig_tstamp == tstamp
                assert len(b'foo') == key_sz
                assert len(b'bar') == value_sz
                assert b'bar' == value
                assert 0 == row_type
                file_info, new_pos = data_file.read(fmap, current_pos)
                crc32, tstamp, key_sz, value_sz, row_type, key, value, pos = file_info
                assert crc32_size + header_size + key_sz + value_sz + current_pos == new_pos
                assert tstamp1 == tstamp
                assert len(b'foo1') == key_sz
                assert len(b'bar1') == value_sz
                assert b'bar1' == value
                assert 1 == row_type
        finally:
            data_file.close()

    def test_read_bad_crc(self, base_dir):
        """Test for read method with a bad crc error."""
        data_file = self.file_class(base_dir)
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
                with pytest.raises(BadCrc):
                    data_file.read(fmap, 0)

    def test_read_bad_header(self, base_dir):
        """Test for read method with a bad header/unpack error."""
        data_file = self.file_class(base_dir)
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
                with pytest.raises(BadHeader):
                    data_file.read(fmap, 0)

    def test_write_after_read_after_write(self, base_dir):
        """write data after a write/read cycle."""
        data1 = os.urandom(200)
        data_file = self.file_class(base_dir)
        try:
            init_pos = data_file.fd.tell()
            tstamp_1, value_1_pos, value_1_sz = data_file.write(0, b'foo', data1)
            header = header_struct.pack(tstamp_1, len(b'foo'), len(data1), 0)
            crc32 = crc32_struct.pack(zlib.crc32(header + b'foo' + data1) & 0xFFFFFFFF)
            assert init_pos + len(crc32 + header) + len(b'foo') == value_1_pos
            init_pos_2 = value_1_pos + len(data1)
            data2 = os.urandom(100)
            tstamp_2, value_2_pos, value_2_sz = data_file.write(0, b'foo1', data2)
            header = header_struct.pack(tstamp_2, len(b'foo1'), len(data2), 0)
            crc32 = crc32_struct.pack(zlib.crc32(header + b'foo1' + data2) & 0xFFFFFFFF)
            assert init_pos_2 + len(crc32 + header) + len(b'foo1') == value_2_pos
            # now read the first value
            value_1 = data_file[value_1_pos:value_1_pos + value_1_sz]
            assert value_1 == data1
            # now write something else, should end up at the end.
            init_pos_3 = value_2_pos + len(data2)
            data3 = os.urandom(100)
            tstamp_3, value_3_pos, value_3_sz = data_file.write(0, b'foo2', data3)
            header = header_struct.pack(tstamp_3, len(b'foo2'), len(data3), 0)
            crc32 = crc32_struct.pack(zlib.crc32(header + b'foo2' + data3) & 0xFFFFFFFF)
            assert init_pos_3 + len(crc32 + header) + len(b'foo2') == value_3_pos
        finally:
            data_file.close()


# ============================================================================
# TempDataFileTest
# ============================================================================

class TestTempDataFile(TestDataFile):
    """Tests for TempDataFile."""

    file_class = TempDataFile

    def test_tempfile_name(self, base_dir):
        """Test the name of the tempfile isn't LIVE."""
        new_file = self.file_class(base_dir)
        try:
            assert LIVE not in new_file.filename
        finally:
            new_file.close()

    def test_make_immutable_and_rename_hint(self, base_dir):
        """Test for make_immutable method."""
        new_file = self.file_class(base_dir)
        try:
            new_file.fd.write(b'foo')
            with new_file.get_hint_file() as hint_file:
                hint_file.fd.write(b'foo,bar')
            immutable_file = new_file.make_immutable()
            try:
                # the DataFile should be closed
                assert new_file.fd is None
                assert immutable_file.file_id == new_file.file_id
                # and the hint should have the new name too
                hint_file = immutable_file.get_hint_file()
                assert os.path.exists(hint_file.path)
                hint_file.close()
                hint_file = new_file.get_hint_file()
                assert not os.path.exists(hint_file.path)
                hint_file.close()
            finally:
                immutable_file.close()
        finally:
            new_file.close()

    def test_delete(self, base_dir):
        """Test for delete method."""
        new_file = self.file_class(base_dir)
        assert os.path.exists(new_file.filename)
        with new_file.get_hint_file():
            pass
        assert os.path.exists(new_file.hint_filename)
        new_file.close()
        new_file.delete()
        assert not os.path.exists(new_file.filename)
        assert not os.path.exists(new_file.hint_filename)


# ============================================================================
# ImmutableDataFileTest
# ============================================================================

class TestImmutableDataFile(TestDataFile):
    """Tests for ImmutableDataFile.

    Inherits from TestDataFile - inherited tests will use file_class = DataFile
    (matching original unittest behavior).
    """

    def test_make_immutable(self, base_dir):
        """Test for make_immutable."""
        new_file = DataFile(base_dir)
        try:
            # write some data
            new_file.fd.write(b'foo')
            immutable_file = new_file.make_immutable()
            try:
                # it's the same instance?
                assert immutable_file.make_immutable() == immutable_file
            finally:
                immutable_file.close()
        finally:
            new_file.close()

    def test_make_zombie(self, base_dir):
        """Test for the make_zombie method."""
        new_file = DataFile(base_dir)
        try:
            # write some data
            new_file.fd.write(b'foo')
            immutable_file = new_file.make_immutable()
            try:
                # create a zombie
                zombie = immutable_file.make_zombie()
                # it's the same instance?
                assert zombie == immutable_file
                assert not zombie.fd.closed
            finally:
                immutable_file.close()
        finally:
            new_file.close()

    def test_make_zombie_with_hint(self, base_dir):
        """Test for the make_zombie method with a hint file.."""
        new_file = DataFile(base_dir)
        try:
            # write some data
            new_file.fd.write(b'foo')
            immutable_file = new_file.make_immutable()
            try:
                immutable_file.get_hint_file().close()
                # create a zombie
                zombie = immutable_file.make_zombie()
                assert zombie.has_hint
                # it's the same instance?
                assert zombie == immutable_file
                assert not zombie.fd.closed
            finally:
                immutable_file.close()
        finally:
            new_file.close()

    def test_write(self, base_dir):
        """Test the write fails on immutable files."""
        new_file = DataFile(base_dir)
        try:
            # write some data
            new_file.fd.write(b'foo')
            immutable_file = new_file.make_immutable()
            try:
                with pytest.raises(NotImplementedError):
                    immutable_file.write(0, b'foo', b'bar')
            finally:
                immutable_file.close()
        finally:
            new_file.close()

    def test__open(self, base_dir):
        """Test the _open private method."""
        new_file = DataFile(base_dir)
        try:
            # write some data
            new_file.fd.write(b'foo')
            immutable_file = new_file.make_immutable()
            try:
                assert immutable_file.fd is not None
                assert immutable_file.fmmap is not None
                # check that the file is opened only for read
                with pytest.raises(IOError):
                    immutable_file.fd.write(b'foo')
            finally:
                immutable_file.close()
        finally:
            new_file.close()

    def test_close(self, base_dir):
        """Test the close method."""
        new_file = DataFile(base_dir)
        try:
            # write some data
            new_file.fd.write(b'foo')
            immutable_file = new_file.make_immutable()
            immutable_file.close()
            assert immutable_file.fd is None
            assert immutable_file.fmmap is None
        finally:
            new_file.close()

    def test_iter_entries(self, base_dir):
        """Test for iter_entries"""
        db = Tritcask(base_dir)
        for i in range(10):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        file_id = db.live_file.file_id
        db.rotate()
        db.shutdown()
        db = Tritcask(base_dir)
        try:
            for i, entry in enumerate(db._immutable[file_id].iter_entries()):
                assert entry[4] == i
                assert entry[5] == ('foo%d' % (i,)).encode('ascii')
                assert entry[6] == ('bar%d' % (i,)).encode('ascii')
        finally:
            db.shutdown()

    def test_iter_entries_bad_crc(self, base_dir, memento):
        """Test that BadCrc during iter_entries is the same as EOF."""
        db = Tritcask(base_dir)
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
        db = Tritcask(base_dir)
        try:
            entries = []
            for i, entry in enumerate(db._immutable[file_id].iter_entries()):
                entries.append(entry)
                assert entry[4] == i
                assert entry[5] == ('foo%d' % (i,)).encode('ascii')
                assert entry[6] == ('bar%d' % (i,)).encode('ascii')
            assert len(entries) == 10
            assert memento.check_warning('Found BadCrc on')
            assert memento.check_warning('the rest of the file will be ignored.')
        finally:
            db.shutdown()

    def test_iter_entries_bad_header_unpack(self, base_dir, memento):
        """Test that unpack error during iter_entries is the same as EOF."""
        db = Tritcask(base_dir)
        for i in range(10):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        # truncate the file at the start of the header of the last value.
        db.live_file.fd.seek(crc32_size + header_size + 8 + crc32_size + 4)
        db.live_file.fd.truncate()
        file_id = db.live_file.file_id
        db.rotate()
        db.shutdown()
        db = Tritcask(base_dir)
        try:
            entries = []
            for i, entry in enumerate(db._immutable[file_id].iter_entries()):
                entries.append(entry)
                assert entry[4] == i
                assert entry[5] == ('foo%d' % (i,)).encode('ascii')
                assert entry[6] == ('bar%d' % (i,)).encode('ascii')
            assert len(entries) == 1
            assert db._immutable[file_id].has_bad_data, 'has_bad_data should be True.'
            assert memento.check_warning('Found corrupted header on')
            assert memento.check_warning('the rest of the file will be ignored.')
        finally:
            db.shutdown()

    def test__getitem__(self, base_dir):
        """Test our slicing support."""
        rw_file = DataFile(base_dir)
        try:
            tstamp, value_pos, value_sz = rw_file.write(0, b'foo', b'bar')
            tstamp1, value1_pos, value1_sz = rw_file.write(1, b'foo1', b'bar1')
            data_file = rw_file.make_immutable()
            try:
                assert data_file[value_pos:value_pos + value_sz] == b'bar'
                assert data_file[value1_pos:value1_pos + value1_sz] == b'bar1'
            finally:
                data_file.close()
        finally:
            rw_file.close()

    def test__getitem__no_slice(self, base_dir):
        """Test that we *only* support slicing."""
        rw_file = DataFile(base_dir)
        try:
            tstamp, value_pos, value_sz = rw_file.write(0, b'foo', b'bar')
            data_file = rw_file.make_immutable()
            try:
                with pytest.raises(ValueError):
                    data_file.__getitem__(value_pos)
            finally:
                data_file.close()
        finally:
            rw_file.close()

    def test_exists(self, base_dir):
        """Tests for exists method."""
        rw_file = DataFile(base_dir)
        try:
            tstamp, value_pos, value_sz = rw_file.write(0, b'foo', b'bar')
            data_file = rw_file.make_immutable()
            try:
                assert data_file.exists()
            finally:
                data_file.close()
        finally:
            rw_file.close()

    def test_size(self, base_dir):
        """Test the size property."""
        rw_file = DataFile(base_dir)
        try:
            tstamp, value_pos, value_sz = rw_file.write(0, b'foo', b'bar')
            data_file = rw_file.make_immutable()
            try:
                assert data_file.size == len(b'bar') + len(b'foo') + header_size + crc32_size
            finally:
                data_file.close()
        finally:
            rw_file.close()

    def test_has_hint(self, base_dir):
        """Test that has_hint works as expected."""
        rw_file = DataFile(base_dir)
        try:
            tstamp, value_pos, value_sz = rw_file.write(0, b'foo', b'bar')
            data_file = rw_file.make_immutable()
            try:
                assert not data_file.has_hint
                data_file.get_hint_file().close()
                assert data_file.has_hint
            finally:
                data_file.close()
        finally:
            rw_file.close()

    def test_hint_size(self, base_dir):
        """Test that hint_size work as expected."""
        rw_file = DataFile(base_dir)
        try:
            tstamp, value_pos, value_sz = rw_file.write(0, b'foo', b'bar')
            data_file = rw_file.make_immutable()
            try:
                assert data_file.hint_size == 0
                hint_file = data_file.get_hint_file()
                hint_file.fd.write(b"some data")
                hint_file.close()
                assert data_file.hint_size == len(b"some data")
            finally:
                data_file.close()
        finally:
            rw_file.close()


# ============================================================================
# DeadDataFileTest
# ============================================================================

class TestDeadDataFile(TestImmutableDataFile):
    """Tests for DeadDataFile.

    Inherits from TestImmutableDataFile - inherited tests will use file_class = DataFile
    (matching original unittest behavior).
    """

    def create_dead_file(self, base_dir):
        """Helper method to create a dead file."""
        rw_file = DataFile(base_dir)
        tstamp, value_pos, value_sz = rw_file.write(0, b'foo', b'bar')
        immutable_file = rw_file.make_immutable()
        immutable_file.get_hint_file().close()
        data_file = immutable_file.make_zombie()
        data_file.close()
        return DeadDataFile(base_dir, os.path.basename(data_file.filename))

    def test_delete(self, base_dir):
        """Test for delete method."""
        dead_file = self.create_dead_file(base_dir)
        assert os.path.exists(dead_file.filename)
        assert os.path.exists(dead_file.hint_filename)
        dead_file.delete()
        assert not os.path.exists(dead_file.filename)
        assert not os.path.exists(dead_file.hint_filename)

    def test_write(self, base_dir):
        """Test that write always fails with NotImplementedError."""
        dead_file = self.create_dead_file(base_dir)
        with pytest.raises(NotImplementedError):
            dead_file.write(b'1')

    def test_read(self, base_dir):
        """Test that read always fails with NotImplementedError."""
        dead_file = self.create_dead_file(base_dir)
        with pytest.raises(NotImplementedError):
            dead_file.read()

    def test__open(self, base_dir):
        """Test that always fails with NotImplementedError."""
        dead_file = self.create_dead_file(base_dir)
        with pytest.raises(NotImplementedError):
            dead_file._open()

    def test_close(self, base_dir):
        """Test that always fails with NotImplementedError."""
        dead_file = self.create_dead_file(base_dir)
        with pytest.raises(NotImplementedError):
            dead_file.close()

    def test_make_immutable(self, base_dir):
        """Test that always fails with NotImplementedError."""
        dead_file = self.create_dead_file(base_dir)
        with pytest.raises(NotImplementedError):
            dead_file.make_immutable()

    def test_make_zombie(self, base_dir):
        """Test that always fails with NotImplementedError."""
        dead_file = self.create_dead_file(base_dir)
        with pytest.raises(NotImplementedError):
            dead_file.make_zombie()

    def test__getitem__(self, base_dir):
        """Test that always fails with NotImplementedError."""
        dead_file = self.create_dead_file(base_dir)
        with pytest.raises(NotImplementedError):
            dead_file.__getitem__()

    def test_iter_entries(self, base_dir):
        """Test that always fails with NotImplementedError."""
        dead_file = self.create_dead_file(base_dir)
        with pytest.raises(NotImplementedError):
            dead_file.iter_entries()

    def test_exists(self, base_dir):
        """Tests for exists method."""
        dead_file = self.create_dead_file(base_dir)
        assert dead_file.exists()

    def test_size(self, base_dir):
        """Test the size property."""
        dead_file = self.create_dead_file(base_dir)
        assert dead_file.size == len(b'bar') + len(b'foo') + header_size + crc32_size


# ============================================================================
# HintFileTest
# ============================================================================

class TestHintFile:
    """Tests for HintFile class."""

    def test_init(self, base_dir):
        """Test initialization."""
        path = os.path.join(base_dir, 'test_hint')
        hint_file = HintFile(path)
        # in Py3 if you open w+b it will have mode rb+ (which is ok)
        assert hint_file.fd.mode in ('w+b', 'rb+')
        hint_file.close()

    def test_init_existing(self, base_dir):
        """Test initialization with existing file."""
        path = os.path.join(base_dir, 'test_hint_existing')
        hint_file = HintFile(path)
        hint_file.fd.write(b"some data")
        hint_file.close()
        hint_file = HintFile(path)
        assert hint_file.fd.mode == 'rb'
        hint_file.close()

    def test_init_existing_empty(self, base_dir):
        """Test initialization with existing file."""
        path = os.path.join(base_dir, 'test_hint_existing')
        hint_file = HintFile(path)
        hint_file.close()
        hint_file = HintFile(path)
        # in Py3 if you open w+b it will have mode rb+ (which is ok)
        assert hint_file.fd.mode in ('w+b', 'rb+')
        hint_file.close()

    def test_close(self, base_dir):
        """Test for the close method."""
        path = os.path.join(base_dir, 'test_hint_close')
        hint_file = HintFile(path)
        # in Py3 if you open w+b it will have mode rb+ (which is ok)
        assert hint_file.fd.mode in ('w+b', 'rb+')
        fd = hint_file.fd
        hint_file.close()
        assert fd.closed

    def test_contextmanager(self, base_dir):
        """Test the context manager protocol."""
        path = os.path.join(base_dir, 'test_hint_close')
        hint_file = HintFile(path)
        with hint_file as hf:
            assert hint_file == hf

    def test_iter_entries(self, base_dir):
        """Test for iter_entries method."""
        db = Tritcask(base_dir)
        # create some stuff.
        for i in range(10):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%d' % (i,)).encode('ascii'))
        # make the data file inactive and generate the hint.
        immutable_file = db.live_file.make_immutable()
        try:
            db.shutdown()
            Tritcask(base_dir).shutdown()
            assert immutable_file.has_hint
            # check that the hint matches the contents in the DB
            hint_file = immutable_file.get_hint_file()
            try:
                db = Tritcask(base_dir)
                try:
                    for hint_entry in hint_file.iter_entries():
                        assert len(hint_entry) == 6
                        assert db.get(hint_entry[2], hint_entry[-1]) is not None
                finally:
                    db.shutdown()
            finally:
                hint_file.close()
        finally:
            immutable_file.close()

    def test_write(self, base_dir):
        """Test the write method."""
        hint_file = HintFile(os.path.join(base_dir, 'hint_file'))
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
            assert tstamp1 == tstamp
            assert len(b'foo') == key_sz
            assert 0 == row_type
            assert len(b'bar') == value_sz
            assert 100 == value_pos
            key = f.read(key_sz)
            assert b'foo' == key
            # read the second entry
            header = hint_header_struct.unpack(f.read(hint_header_size))
            tstamp, key_sz, row_type, value_sz, value_pos = header
            assert tstamp2 == tstamp
            assert len('foo1') == key_sz
            assert 1 == row_type
            assert len('bar1') == value_sz
            assert 100 == value_pos
            key = f.read(key_sz)
            assert b'foo1' == key


# ============================================================================
# HintEntryTest
# ============================================================================

class TestHintEntry:
    """Tests for HintEntry class."""

    def test_header_property(self):
        """Test the header property."""
        tstamp = timestamp()
        entry = HintEntry(tstamp, len('foo'), 0, len('bar'), 100, 'foo')
        assert (entry.tstamp, entry.key_sz, entry.row_type,
                entry.value_sz, entry.value_pos) == entry.header


# ============================================================================
# LowLevelTest
# ============================================================================

class TestLowLevel:
    """Tests for low level methods and functions."""

    def test_get_file_id(self, base_dir):
        """Test for _get_file_id function."""
        db = Tritcask(base_dir)
        try:
            filename = os.path.split(db.live_file.filename)[1]
            assert db.live_file.file_id == _get_file_id(filename)
        finally:
            db.shutdown()

    def test_is_hint(self):
        """Test for is_hint function."""
        filename = DataFile._get_next_file_id() + LIVE
        assert not is_hint(filename)
        filename = DataFile._get_next_file_id() + HINT
        assert is_hint(filename)

    def test_is_immutable(self):
        """Test for is_immutable function."""
        filename = DataFile._get_next_file_id() + LIVE
        assert not is_immutable(filename)
        filename = DataFile._get_next_file_id() + INACTIVE
        assert is_immutable(filename)

    def test_is_live(self):
        """Test for is_live function."""
        filename = DataFile._get_next_file_id() + HINT
        assert not is_live(filename)
        filename = DataFile._get_next_file_id() + LIVE
        assert is_live(filename)

    def test_get_value(self, base_dir):
        """Test _get_value method."""
        db = Tritcask(base_dir)
        try:
            db.put(0, b'foo', b'bar')
            value = db._get_value(
                db.live_file.file_id, crc32_size + header_size + len(b'foo'),
                len(b'bar'))
            assert value == b'bar'
        finally:
            db.shutdown()

    def test_get_value_different_file_ids(self, base_dir):
        """Test _get_value with different file_id."""
        db = Tritcask(base_dir)
        try:
            db.put(0, b'foo', b'bar')
            old_file_id = db.live_file.file_id
            # shutdown and rename the file.
            immutable_file = db.live_file.make_immutable()
            try:
                db.shutdown()
                db = Tritcask(base_dir)
                db.put(1, b'foo1', b'bar1')
                assert len(db._immutable) == 1
                # read from the old file
                value = db._get_value(
                    old_file_id, crc32_size + header_size + len(b'foo'), len(b'bar'))
                assert value == b'bar'
                # read from the current file
                value = db._get_value(db.live_file.file_id,
                                      crc32_size + header_size + len(b'foo1'),
                                      len(b'bar1'))
                assert value == b'bar1'
            finally:
                immutable_file.close()
        finally:
            db.shutdown()

    def test_shutdown(self, base_dir):
        """Test shutdown."""
        db = Tritcask(base_dir)
        # create 1 inactive files, with 5 items each
        for i in range(5):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        # make the data file inactive and generate the hint.
        immutable_file = db.live_file.make_immutable()
        try:
            db.shutdown()
            db = Tritcask(base_dir)
            for i in range(5, 10):
                db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
            # iterate over all values, in order to open all inactive data
            # files.
            for rtype, key in db._keydir.keys():
                db.get(rtype, key)
            # check that we have 2 open files
            try:
                assert len(db._immutable) == 1
                assert db.live_file is not None
            finally:
                db.shutdown()
            assert db.live_file is None
            assert len(db._immutable) == 0
        finally:
            immutable_file.close()

    def test_rotate_files(self, base_dir):
        """Test data file rotation."""
        db = Tritcask(base_dir)
        try:
            # add a key/value
            db.put(0, b'foo', b'bar')
            files = sorted(os.listdir(db.base_path))
            assert len(files) == 1
            db.rotate()
            files = sorted(os.listdir(db.base_path))
            assert len(files) == 2
            assert INACTIVE in files[0]
            assert LIVE in files[1]
            # add a new value to trigger the creation of the new file
            db.put(0, b'foo', b'bar1')
            assert len(files) == 2
            assert INACTIVE in files[0]
            assert LIVE in files[1]
        finally:
            db.shutdown()

    def test_rotate_empty_live_file(self, base_dir):
        """Test data file rotation with empty live file."""
        db = Tritcask(base_dir)
        try:
            db.rotate()
            files = sorted(os.listdir(db.base_path))
            assert len(files) == 1
            assert LIVE in files[0]
            assert db.live_file.filename == os.path.join(db.base_path, files[0])
        finally:
            db.shutdown()

    def test_rotate_missing_live_file(self, base_dir):
        """Test data file rotation without a live file."""
        db = Tritcask(base_dir)
        try:
            os.unlink(db.live_file.filename)
            db.rotate()
            files = sorted(os.listdir(db.base_path))
            assert len(files) == 0
        finally:
            db.shutdown()


# ============================================================================
# InitTest
# ============================================================================

class TestInit:
    """Init tests."""

    def test_initialize_new(self, tmp_path):
        """Simple initialization."""
        base_dir = str(tmp_path / "new_dir")
        db = Tritcask(base_dir)
        db.shutdown()

    def test_initialize_existing_empty(self, base_dir):
        """Initialize with an empty existing db."""
        db = Tritcask(base_dir)
        db.shutdown()
        db = Tritcask(base_dir)
        db.shutdown()

    def test_initialize_existing(self, base_dir, memento):
        """Initialize with a existing db."""
        db = Tritcask(base_dir)
        for i in range(10):
            key, value = build_data()
            db.put(0, key, value)
        db.shutdown()
        db = Tritcask(base_dir)
        try:
            assert memento.check_debug(
                'loading entries from (build_hint=%s): %s'
                % (False, db.live_file.filename))
        finally:
            db.shutdown()

    def test_initialize_bad_path(self, base_dir):
        """Initialize with a invalid path."""
        path = os.path.join(base_dir, 'foo')
        open(path, 'w').close()
        with pytest.raises(ValueError):
            Tritcask(path)

    def test_find_data_files(self, base_dir):
        """Test the _find_data_files method."""
        filenames = []
        for i in range(10):
            data_file = DataFile(base_dir)
            data_file.write(i, ('foo%d' % (i,)).encode('ascii'), ('bar%d' % (i,)).encode('ascii'))
            immutable_file = data_file.make_immutable()
            filenames.append(immutable_file.filename)
            immutable_file.close()
        db = Tritcask(base_dir)
        try:
            assert db.live_file.filename not in filenames
            files = [fn.filename for fn in sorted(db._immutable.values(),
                                                  key=attrgetter('filename'))]
            assert files == filenames
        finally:
            db.shutdown()

    def test_find_data_files_immutable_open_error(self, base_dir, monkeypatch, memento):
        """Test the _find_data_files method failing to open a file."""
        data_file = DataFile(base_dir)
        data_file.write(0, b'foo_0', b'bar_0')
        immutable_file = data_file.make_immutable()
        immutable_file.close()

        # patch the open call and make it fail
        def fail_open(self, *a):
            """Always fail."""
            raise IOError("I'm a broken file.")
        monkeypatch.setattr(ImmutableDataFile, '_open', fail_open)
        db = Tritcask(base_dir)
        try:
            files = [fn.filename for fn in sorted(db._immutable.values(),
                                                  key=attrgetter('filename'))]
            assert immutable_file.filename not in files
            # check the logs
            msg = ("Failed to open %s, renaming it to: %s - error: %s" %
                   (immutable_file.filename,
                    immutable_file.filename.replace(INACTIVE, BROKEN),
                    IOError("I'm a broken file")))
            assert memento.check_warning(msg)
        finally:
            db.shutdown()

    def test_find_data_files_live_open_error(self, base_dir, monkeypatch, memento):
        """Test the _find_data_files method failing to open a file."""
        data_file = DataFile(base_dir)
        data_file.write(0, b'foo_0', b'bar_0')
        data_file.close()
        # patch the open call and make it fail
        orig_open = DataFile._open

        def fail_open(self, *a):
            """Always fail only once."""
            monkeypatch.setattr(DataFile, '_open', orig_open)
            raise IOError("I'm a broken file.")
        monkeypatch.setattr(DataFile, '_open', fail_open)
        db = Tritcask(base_dir)
        try:
            assert data_file.filename != db.live_file.filename
            # check the logs
            memento.debug = True
            msg = ("Failed to open %s, renaming it to: %s - error: %s" %
                   (data_file.filename,
                    data_file.filename.replace(LIVE, BROKEN),
                    IOError("I'm a broken file")))
            assert memento.check_warning(msg)
        finally:
            db.shutdown()

    def test_build_keydir_on_init(self, base_dir):
        """Test _build_keydir method."""
        db = Tritcask(base_dir)
        # create some stuff.
        for i in range(10):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        db.shutdown()
        old_keydir = db._keydir
        db = Tritcask(base_dir)
        try:
            # check that the keydir is the same.
            assert old_keydir == db._keydir
        finally:
            db.shutdown()

    def test_build_keydir_with_bad_data(self, base_dir):
        """Test _build_keydir method with a bad data file."""
        db = Tritcask(base_dir)
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
        db = Tritcask(base_dir)
        try:
            # check that the keydir is the expected:
            # the new keydir should have entries from 0-8
            # remove the extra entry from the old keydir
            old_keydir.pop((9, b'foo9'))
            assert old_keydir == db._keydir
        finally:
            db.shutdown()

    def test_build_keydir_without_data(self, base_dir):
        """Test _build_keydir method with an empty data file."""
        db = Tritcask(base_dir)
        try:
            # fake an empty immutable file
            db._immutable[db.live_file.file_id] = db.live_file
            db._build_keydir()
        finally:
            db.shutdown()

    def test_build_keydir_with_hint(self, base_dir):
        """Test _build_keydir using a hint file."""
        db = Tritcask(base_dir)
        # create some stuff.
        for i in range(10):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        # make the data file inactive and generate the hint.
        db.live_file.make_immutable().close()
        old_keydir = db._keydir
        db.shutdown()
        Tritcask(base_dir).shutdown()
        # now create a new DB using the hint
        db = Tritcask(base_dir)
        try:
            assert old_keydir == db._keydir
        finally:
            db.shutdown()

    def test_build_keydir_with_multiple_hint(self, base_dir):
        """Test _build_keydir using a several hint files."""
        db = Tritcask(base_dir)
        # create some stuff.
        for i in range(10):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        # make the data file inactive and generate the hint.
        immutable_file = db.live_file.make_immutable()
        try:
            db.shutdown()
            # create more stuff.
            db = Tritcask(base_dir)
            for i in range(20, 30):
                db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
            # make the data file inactive and generate the hint.
            db.live_file.make_immutable().close()
            old_keydir = db._keydir
            db.shutdown()
            Tritcask(base_dir).shutdown()
            # now create a new DB using the hint
            db = Tritcask(base_dir)
            try:
                assert old_keydir == db._keydir
            finally:
                db.shutdown()
        finally:
            immutable_file.close()

    def test_build_keydir_build_hint(self, base_dir):
        """Test _build_keydir build the hints"""
        db = Tritcask(base_dir)
        # create some stuff.
        for i in range(10):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        # make the data file inactive and generate the hint.
        immutable_file = db.live_file.make_immutable()
        hint_filename = immutable_file.hint_filename
        immutable_file.close()
        db.shutdown()
        db = Tritcask(base_dir)
        try:
            assert os.path.exists(hint_filename)
            # check that the hint matches the contents in the DB
            hint_file = HintFile(hint_filename)
            for hint_entry in hint_file.iter_entries():
                assert len(hint_entry) == 6
                assert db.get(hint_entry[2], hint_entry[-1]) is not None
        finally:
            db.shutdown()

    def test_build_keydir_build_hint_only_for_immutable(self, base_dir):
        """Test that _build_keydir build the hint only for immutable files."""
        db = Tritcask(base_dir)
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
        db = Tritcask(base_dir)
        try:
            assert not os.path.exists(db.live_file.hint_filename)
            assert os.path.exists(hint_filename), os.listdir(db.base_path)
            # check that the hint matches the contents in the DB
            hint_file = HintFile(hint_filename)
            for hint_entry in hint_file.iter_entries():
                assert len(hint_entry) == 6
                assert db.get(hint_entry[2], hint_entry[-1]) is not None
        finally:
            db.shutdown()

    def test_build_keydir_with_empty_hint(self, base_dir):
        """Test _build_keydir using a hint file."""
        db = Tritcask(base_dir)
        # create some stuff.
        for i in range(10):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        # make the data file inactive and generate the hint.
        db.live_file.make_immutable().close()
        old_keydir = db._keydir
        db.shutdown()
        db = Tritcask(base_dir)
        hint_filename = list(db._immutable.values())[0].hint_filename
        # truncate the hint file
        open(hint_filename, 'w').close()
        db.shutdown()
        # open the tritcask and make sure it regenerates the hint file
        db = Tritcask(base_dir)
        try:
            assert os.path.exists(hint_filename), os.listdir(db.base_path)
            assert old_keydir == db._keydir
        finally:
            db.shutdown()

    def test_build_keydir(self, base_dir):
        """Test _build_keydir method."""
        db = Tritcask(base_dir)
        # create some stuff.
        for i in range(10):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        old_keydir = db._keydir
        # shutdown (close the files)
        db.shutdown()
        db = Tritcask(base_dir)
        try:
            assert old_keydir == db._keydir
        finally:
            db.shutdown()

    def test_build_keydir_with_dead_rows(self, base_dir):
        """Test _build_keydir method with TOMBSTONE rows."""
        db = Tritcask(base_dir)
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
        db = Tritcask(base_dir)
        try:
            assert old_keydir == db._keydir
        finally:
            db.shutdown()

    def test_build_keydir_build_hint_with_dead_rows(self, base_dir):
        """Test that _build_keydir build the hint with TOMBSTONE rows."""
        db = Tritcask(base_dir, dead_bytes_threshold=0.1)
        # create some stuff.
        for i in range(100):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        # create a inactive file with all the items
        db.rotate()
        db.shutdown()
        # delete half of the items
        db = Tritcask(base_dir, dead_bytes_threshold=0.01)
        for i in range(50):
            db.delete(i, ('foo%d' % (i,)).encode('ascii'))
        # add one item so the hint file have at least one
        db.put(100, b'foo100', b'bar100')
        assert len(db._keydir.keys()) == 51
        old_keydir = db._keydir
        db.shutdown()
        # trigger a rotate and a hint build
        db = Tritcask(base_dir, dead_bytes_threshold=0.01)
        db.rotate()
        db.shutdown()
        # trigger a hint build of the last rotated file
        Tritcask(base_dir, dead_bytes_threshold=0.01).shutdown()
        db = Tritcask(base_dir, dead_bytes_threshold=0.01)
        try:
            assert sorted(old_keydir.keys()) == sorted(db._keydir.keys())
        finally:
            db.shutdown()

    def test_should_rotate(self, base_dir):
        """Test should_rotate method."""
        db = Tritcask(base_dir)
        try:
            # add some data
            for i in range(10):
                db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
            assert not db.should_rotate()
            # overwrite it 4 times
            for j in range(4):
                for i in range(10):
                    db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
            assert db.should_rotate()
        finally:
            db.shutdown()

    def test_should_merge(self, base_dir):
        """Test should_merge method."""
        db = Tritcask(base_dir)
        try:
            # add some data
            for i in range(10):
                db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
            db.rotate()
            assert not db.should_merge(db._immutable)
            # overwrite it 4 times
            for j in range(4):
                for i in range(10):
                    db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
                db.rotate()
            assert db.should_merge(db._immutable)
        finally:
            db.shutdown()

    def test_should_merge_max_files(self, base_dir):
        """Test should_merge method based on max_im files ."""
        db = Tritcask(base_dir, max_immutable_files=10)
        try:
            # add some data
            # create 10 immutable files
            for j in range(10):
                for i in range(2):
                    db.put(i * j, ('foo%d' % (i * j + i,)).encode('ascii'),
                           ('bar%s' % (i,)).encode('ascii'))
                db.rotate()
            assert not db.should_merge(db._immutable)
            # create the 11th immutable file
            db.put(200, b'foo200', b'bar200')
            db.rotate()
            assert db.should_merge(db._immutable)
        finally:
            db.shutdown()

    def test_should_merge_no_stats(self, base_dir):
        """Test should_merge method without stats for a data file."""
        db = Tritcask(base_dir, max_immutable_files=10)
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
        db = Tritcask(base_dir, auto_merge=False)
        try:
            # check that we don't have any stats for the file with the tombstones
            with pytest.raises(KeyError):
                db._keydir.get_stats(fid)
            # check the should_merge works as expected
            assert db.should_merge(db._immutable)
        finally:
            db.shutdown()

    def test__rotate_and_not_merge(self, base_dir, monkeypatch):
        """Test _rotate_and_merge method."""
        db = Tritcask(base_dir)
        # add the same data in 5 different data files
        for i in range(20):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        db.rotate()
        for j in range(5):
            for i in range(10):
                db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        called = []
        db.shutdown()
        monkeypatch.setattr(Tritcask, 'rotate', lambda *a, **k: called.append('rotate'))
        monkeypatch.setattr(Tritcask, 'merge', lambda *args: called.append('merge'))
        Tritcask(base_dir).shutdown()
        assert 'rotate' in called
        assert 'merge' not in called

    def test__rotate_and_merge(self, base_dir, monkeypatch):
        """Test _rotate_and_merge method."""
        db = Tritcask(base_dir)
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
        monkeypatch.setattr(Tritcask, 'rotate', lambda *a, **k: called.append('rotate'))
        monkeypatch.setattr(Tritcask, 'merge', lambda *args: called.append('merge'))
        Tritcask(base_dir).shutdown()
        assert 'rotate' in called
        assert 'merge' in called

    def test_rotate_on_bad_crc(self, base_dir, monkeypatch):
        """Test that the live file is rotated when a BadCrc is found."""
        db = Tritcask(base_dir)
        # add some data
        for i in range(10):
            db.put(i, ('foo%d' % (i,)).encode('ascii'), ('bar%s' % (i,)).encode('ascii'))
        assert not db.should_rotate()
        # write a different value -> random bytes
        # now write some garbage to the end of file
        db.live_file.fd.write(os.urandom(100))
        db.live_file.fd.flush()
        db.shutdown()
        called = []
        monkeypatch.setattr(Tritcask, 'rotate', lambda *a, **k: called.append('rotate'))
        Tritcask(base_dir).shutdown()
        assert 'rotate' in called


# ============================================================================
# BasicTest
# ============================================================================

class TestBasic:
    """Basic tests for Tritcask."""

    def test_put(self, base_dir):
        """Basic test for the put method."""
        db = Tritcask(base_dir)
        try:
            key, data = build_data()
            db.put(0, key, data)
            assert len(db._keydir.keys()) == 1
            # check that the entry is in the file
            with open(db.live_file.filename, 'r+b') as f:
                raw_data_len = (len(key) + len(data) + crc32_size + header_size)
                raw_data = f.read(raw_data_len)
                header = header_struct.unpack(
                    raw_data[crc32_size:crc32_size + header_size])
                tstamp, key_sz, value_sz, row_type = header
                assert data == raw_data[crc32_size + header_size + key_sz:]
            assert db.get(0, key) == data
            # add new entry and check
            key_1, data_1 = build_data()
            db.put(1, key_1, data_1)
            assert len(db._keydir.keys()) == 2
            assert db.get(0, key) == data
            assert db.get(1, key_1) == data_1
        finally:
            db.shutdown()

    def test_delete(self, base_dir):
        """Basic test for the delete method."""
        db = Tritcask(base_dir)
        try:
            key, data = build_data()
            db.put(0, key, data)
            db.delete(0, key)
            # check that the TOMBSTONE is there for these keys
            with open(db.live_file.filename, 'r+b') as f:
                raw_data_len = len(key) + len(TOMBSTONE) + crc32_size + header_size
                f.seek(-1 * raw_data_len, os.SEEK_END)
                raw_data = f.read(raw_data_len)
                assert TOMBSTONE == raw_data[crc32_size + header_size + len(key):]
        finally:
            db.shutdown()

    def test_delete_stats_updated(self, base_dir):
        """Test that calling delete update the keydir stats."""
        db = Tritcask(base_dir)
        try:
            key, data = build_data()
            db.put(0, key, data)
            stats = db._keydir.get_stats(db.live_file.file_id)
            assert stats['live_entries'] == 1
            db.delete(0, key)
            stats = db._keydir.get_stats(db.live_file.file_id)
            assert stats['live_entries'] == 0
            assert stats['live_bytes'] == 0
        finally:
            db.shutdown()

    def test_get(self, base_dir):
        """Basic test for the get method."""
        db = Tritcask(base_dir)
        try:
            key, data = build_data()
            db.put(0, key, data)
            assert db.get(0, key) == data
            db.delete(0, key)
        finally:
            db.shutdown()

    def test_put_only_bytes_key(self, base_dir):
        """Test that put only works with bytes keys."""
        db = Tritcask(base_dir)
        try:
            _, data = build_data()
            with pytest.raises(ValueError):
                db.put(0, None, data)
            with pytest.raises(ValueError):
                db.put(0, u'foo', data)
            with pytest.raises(ValueError):
                db.put(0, object(), data)
        finally:
            db.shutdown()

    def test_put_only_bytes_value(self, base_dir):
        """Test that put only works with bytes keys."""
        db = Tritcask(base_dir)
        try:
            key, _ = build_data()
            with pytest.raises(ValueError):
                db.put(0, key, None)
            with pytest.raises(ValueError):
                db.put(0, key, u'foo')
            with pytest.raises(ValueError):
                db.put(0, key, object())
        finally:
            db.shutdown()

    def test_get_only_bytes(self, base_dir):
        """Test that get only works with bytes."""
        db = Tritcask(base_dir)
        try:
            key, data = build_data()
            db.put(0, key, data)
            with pytest.raises(ValueError):
                db.get(0, None)
            with pytest.raises(ValueError):
                db.get(0, u'foobar')
            with pytest.raises(ValueError):
                db.get(0, object())
        finally:
            db.shutdown()

    def test_delete_only_bytes(self, base_dir):
        """Test that delete only works with bytes."""
        db = Tritcask(base_dir)
        try:
            key, data = build_data()
            with pytest.raises(ValueError):
                db.delete(0, None)
            with pytest.raises(ValueError):
                db.delete(0, u'foobar')
            with pytest.raises(ValueError):
                db.delete(0, object())
        finally:
            db.shutdown()

    def test_keys(self, base_dir):
        """Test for the keys() method."""
        db = Tritcask(base_dir)
        try:
            # add some values
            key, data = build_data()
            db.put(0, key, data)
            key, data = build_data()
            db.put(0, key, data)
            assert db._keydir.keys() == db.keys()
        finally:
            db.shutdown()

    def test__contains__(self, base_dir):
        """Test for __contains__ method."""
        db = Tritcask(base_dir)
        try:
            key, data = build_data()
            db.put(0, key, data)
            key1, data1 = build_data()
            db.put(0, key1, data1)
            assert (0, key) in db._keydir
            assert (0, key) in db
            assert (0, key1) in db._keydir
            assert (0, key1) in db
        finally:
            db.shutdown()


# ============================================================================
# MergeTests
# ============================================================================

class TestMerge:
    """Tests for merge functionality."""

    def _add_data(self, db, size=100):
        """Add random data to a db."""
        for i in range(size):
            key, value = build_data()
            db.put(0, key, value)

    def test_simple_merge(self, base_dir):
        """Test a simple merge."""
        # create 3 immutable files
        db = Tritcask(base_dir)
        self._add_data(db, 100)
        db.rotate()
        self._add_data(db, 100)
        db.rotate()
        self._add_data(db, 100)
        db.shutdown()
        db = Tritcask(base_dir)
        try:
            keydir_to_merge = db._keydir.copy()
            # add data to the live file
            self._add_data(db, 100)
            immutable_fnames = [ifile.filename for ifile in db._immutable.values()]
            # do the merge.
            data_file = db.merge(db._immutable)
            try:
                for entry in data_file.iter_entries():
                    (crc32, tstamp, key_sz, value_sz, row_type,
                        key, value, value_pos) = entry
                    keydir_entry = keydir_to_merge[(row_type, key)]
                    (old_file_id, old_tstamp, old_value_sz, old_value_pos) = keydir_entry
                    old_value = db._get_value(old_file_id, old_value_pos, old_value_sz)
                    assert old_value == value
                for fname in immutable_fnames:
                    assert not os.path.exists(fname)
                    assert os.path.exists(fname.replace(INACTIVE, DEAD))
            finally:
                data_file.close()
        finally:
            db.shutdown()

    def test_single_file_merge(self, base_dir):
        """Test a single file merge (a.k.a compactation)."""
        # create 3 immutable files
        db = Tritcask(base_dir)
        self._add_data(db, 100)
        db.rotate()
        self._add_data(db, 100)
        db.rotate()
        self._add_data(db, 100)
        db.shutdown()
        db = Tritcask(base_dir)
        try:
            keydir_to_merge = db._keydir.copy()
            # add data to the live file
            self._add_data(db, 100)
            immutable_fnames = [ifile.filename for ifile in db._immutable.values()]
            # do the merge.
            data_file = db.merge(db._immutable)
            try:
                for entry in data_file.iter_entries():
                    (crc32, tstamp, key_sz, value_sz, row_type,
                        key, value, value_pos) = entry
                    keydir_entry = keydir_to_merge[(row_type, key)]
                    (old_file_id, old_tstamp, old_value_sz, old_value_pos) = keydir_entry
                    old_value = db._get_value(old_file_id, old_value_pos, old_value_sz)
                    assert old_value == value
                for fname in immutable_fnames:
                    assert not os.path.exists(fname)
                    assert os.path.exists(fname.replace(INACTIVE, DEAD))
            finally:
                data_file.close()
        finally:
            db.shutdown()

    def test_single_auto_merge(self, base_dir):
        """Test auto-merge in startup."""
        db = Tritcask(base_dir)
        self._add_data(db, 100)
        # delete almost all entries to trigger a merge
        for i, k in enumerate(list(db._keydir.keys())):
            if i <= 90:
                db.delete(*k)
        old_live_file = db.live_file
        db.shutdown()
        # at this moment we only have the live file
        # start a new Tritcask instance.
        db = Tritcask(base_dir)
        try:
            assert not old_live_file.exists()
            old_live_file.close()
            assert old_live_file.file_id in db._immutable
        finally:
            db.shutdown()

    def test_auto_merge(self, base_dir):
        """Test auto-merge in startup."""
        db = Tritcask(base_dir)
        self._add_data(db, 100)
        # delete almost all entries to trigger a merge
        for i, k in enumerate(list(db._keydir.keys())):
            if i <= 90:
                db.delete(*k)
        old_live_file = db.live_file
        db.rotate()
        self._add_data(db, 100)
        # delete almost all entries to trigger a merge
        for i, k in enumerate(list(db._keydir.keys())):
            if i <= 90:
                db.delete(*k)
        old_live_file_1 = db.live_file
        db.shutdown()
        # at this moment we only have the live file
        # start a new Tritcask instance.
        db = Tritcask(base_dir)
        try:
            assert not old_live_file.exists()
            assert not old_live_file_1.exists()
            assert old_live_file.file_id in db._immutable
            assert old_live_file_1.file_id in db._immutable
        finally:
            db.shutdown()

    def test_merge_do_nothing_all_dead_entries_single_file(self, base_dir):
        """Test possible merge of a immutable file with 100% dead data."""
        db = Tritcask(base_dir)
        self._add_data(db, 100)
        # delete almost all entries to trigger a merge
        for i, k in enumerate(list(db._keydir.keys())):
            if i < 100:
                db.delete(*k)
        db.shutdown()
        # start tritcask without automerge and check
        db = Tritcask(base_dir, auto_merge=False)
        files = sorted(os.listdir(db.base_path))
        try:
            assert len(files) == 2
            assert INACTIVE in files[0]
            assert LIVE in files[1]
        finally:
            db.shutdown()
        # trigger the rotation
        # now we should have a single immutable_file (previous live one)
        db = Tritcask(base_dir)
        try:
            assert len(db._immutable) == 1
            files = sorted(os.listdir(db.base_path))
            assert len(files) == 3
            assert DEAD in files[0]
            assert DEAD in files[1]
            assert LIVE in files[2]
        finally:
            db.shutdown()
        # at this moment we have the live + imm_file files
        # start a new Tritcask instance.
        db = Tritcask(base_dir)
        try:
            files = sorted(os.path.join(base_dir, f) for f in os.listdir(db.base_path))
            # the immutable_file should be dead as there are no live entries
            # there should be only 2 files, the dead and the live
            # dead + dead_hint + live
            files = sorted(os.listdir(db.base_path))
            assert len(files) == 1, files
            assert LIVE in files[0]
        finally:
            db.shutdown()
        # start a new Tritcask to check everything is ok after the merge.
        Tritcask(base_dir).shutdown()
        # check that the dead file is no more
        files = sorted(os.listdir(db.base_path))
        assert len(files) == 1, files

    def test_merge_mixed_dead_entries(self, base_dir):
        """Test merge of 2 immutable files one with 100%% dead data."""
        db = Tritcask(base_dir)
        self._add_data(db, 100)
        # delete almost all entries to trigger a merge
        for i, k in enumerate(list(db._keydir.keys())):
            if i <= 100:
                db.delete(*k)
        db.shutdown()
        db = Tritcask(base_dir, auto_merge=False)
        try:
            assert len(db._immutable) == 1
            imm_file_1 = list(db._immutable.values())[0]
        finally:
            db.shutdown()
        db = Tritcask(base_dir)
        # the "empty" immutable_file should be dead as there are no live entries
        # but it's also removed from the _immutable dict
        files = sorted(os.path.join(base_dir, f) for f in os.listdir(db.base_path))
        try:
            assert imm_file_1.filename.replace(INACTIVE, DEAD) in files
            self._add_data(db, 100)
            for i, k in enumerate(list(db._keydir.keys())):
                if i % 2:
                    db.delete(*k)
        finally:
            db.shutdown()
        # at this moment we only have the live file
        # start a new Tritcask instance.
        db = Tritcask(base_dir)
        # there should be 5 files:
        #    - the immutable file just merged as a DEAD file.
        #    - the new immutable_file
        #    - the current live file
        files = sorted(os.listdir(db.base_path))
        try:
            assert len(files) == 4, files
            assert DEAD in files[0]
            assert INACTIVE in files[1]
            assert INACTIVE in files[2]
            assert HINT in files[2]
            assert LIVE in files[3]
        finally:
            db.shutdown()
        # start a new Tritcask to check everything is ok after the merge.
        Tritcask(base_dir).shutdown()
        # check that the dead files were deleted
        files = sorted(os.listdir(db.base_path))
        assert len(files) == 3, files
        assert INACTIVE in files[0]
        assert INACTIVE in files[1]
        assert HINT in files[1]
        assert LIVE in files[2]

    def test_merged_file_is_older_than_live(self, base_dir):
        """Test that merge creates and live file newer than the merge result."""
        db = Tritcask(base_dir)
        self._add_data(db, 100)
        # delete almost all entries to trigger a merge
        for i, k in enumerate(list(db._keydir.keys())):
            if i <= 60:
                db.delete(*k)
        db.shutdown()
        # trigger the rotation
        # now we should have a single immutable_file (previous live one)
        db = Tritcask(base_dir)
        try:
            assert len(db._immutable) == 1
            dead_file = list(db._immutable.values())[0]
            assert DEAD in dead_file.filename
            files = sorted(os.listdir(db.base_path))
            assert len(files) == 4
            assert DEAD in files[0]
            assert INACTIVE in files[1]
            assert HINT in files[2]
            assert LIVE in files[3]
            assert db.live_file.size == 0
        finally:
            db.shutdown()
        # at this moment we have the live + imm_file files
        # start a new Tritcask instance.
        db = Tritcask(base_dir)
        try:
            # the dead file should be gone
            # there should be only 2 files, the immutable and the live
            # immutable + hint + live
            files = sorted(os.listdir(db.base_path))
            assert len(files) == 3, files
            assert INACTIVE in files[0]
            assert HINT in files[1]
            assert LIVE in files[2]
            assert len(db._immutable) == 1, "More than 1 immutable file."
            live_id = int(db.live_file.file_id)
            imm_id = int(list(db._immutable.keys())[0])
            assert live_id > imm_id, "%d <= %d" % (live_id, imm_id)
        finally:
            db.shutdown()


# ============================================================================
# KeydirStatsTests
# ============================================================================

class TestKeydirStats:
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
        assert keydir._stats[file_id]['live_bytes'] == entry_size * 10
        assert keydir._stats[file_id_1]['live_bytes'] == entry_size * 20

    def test_update_entry(self):
        """Test that __setitem__ updates the stats for an entry."""
        keydir = Keydir()
        file_id = DataFile._get_next_file_id()
        key = str(uuid.uuid4())
        entry = KeydirEntry(file_id, timestamp(), 1, 1)
        keydir[(0, key)] = entry
        base_size = len(key) + header_size + crc32_size
        assert keydir._stats[file_id]['live_bytes'] == base_size + 1
        new_entry = KeydirEntry(file_id, timestamp(), 2, 2)
        keydir[(0, key)] = new_entry
        assert keydir._stats[file_id]['live_bytes'] == base_size + 2

    def test_update_entry_different_file_id(self):
        """Test that __setitem__ updates the stats for an entry."""
        keydir = Keydir()
        file_id = DataFile._get_next_file_id()
        key = str(uuid.uuid4())
        entry = KeydirEntry(file_id, timestamp(), 10, 1)
        keydir[(0, key)] = entry
        base_size = len(key) + header_size + crc32_size
        assert keydir._stats[file_id]['live_bytes'] == base_size + 10
        new_file_id = DataFile._get_next_file_id()
        new_entry = KeydirEntry(new_file_id, timestamp(), 5, 10)
        keydir[(0, key)] = new_entry
        assert keydir._stats[new_file_id]['live_bytes'] == base_size + 5
        assert keydir._stats[file_id]['live_bytes'] == 0

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
        assert keydir._stats[file_id]['live_bytes'] == entry_size * (10 / 2)
        assert keydir._stats[file_id_1]['live_bytes'] == entry_size * (20 / 2)

    def test_remove_missing_key(self):
        """Test the remove method with a missing key."""
        keydir = Keydir()
        key = str(uuid.uuid4())
        try:
            keydir.remove((0, key))
        except KeyError as e:
            pytest.fail(str(e))

    def test_remove_missing_stat_key(self):
        """Test the remove method with a missing key."""
        keydir = Keydir()
        file_id = DataFile._get_next_file_id()
        key = str(uuid.uuid4())
        keydir[(0, key)] = KeydirEntry(file_id, timestamp(),
                                       len(str(uuid.uuid4())), 10)
        del keydir._stats[file_id]
        try:
            keydir.remove((0, key))
        except Exception as e:
            pytest.fail(str(e))
        # all good, no exceptions

    def test_get_stats(self):
        """Test the get_stats method."""
        keydir = Keydir()
        file_id = DataFile._get_next_file_id()
        for i in range(10):
            keydir[(0, str(uuid.uuid4()))] = KeydirEntry(
                file_id, timestamp(), len(str(uuid.uuid4())), i + 10)
        assert keydir._stats[file_id] == keydir.get_stats(file_id)
        assert keydir._stats[file_id] is not keydir.get_stats(file_id)

    def test_get_stats_missing(self):
        """Test get_stats with a missing file_id."""
        keydir = Keydir()
        file_id = DataFile._get_next_file_id()
        with pytest.raises(KeyError):
            keydir.get_stats(file_id)


# ============================================================================
# TritcaskShelfTests
# ============================================================================

class TestTritcaskShelf:
    """Tests for TritcaskShelf."""

    def test_invalid_keys(self, base_dir):
        """Test the exception raised when invalid keys are used ('', None)."""
        path = os.path.join(base_dir, 'shelf_invalid_keys')
        shelf = TritcaskShelf(0, Tritcask(path))
        try:
            with pytest.raises(ValueError):
                shelf.__setitem__(None, 'foo')
            with pytest.raises(ValueError):
                shelf.__setitem__('', 'foo')
        finally:
            shelf._db.shutdown()

    def test_contains(self, base_dir):
        """Test that it behaves with the 'in'."""
        path = os.path.join(base_dir, 'shelf_contains')
        shelf = TritcaskShelf(0, Tritcask(path))
        try:
            shelf[b"foo"] = b"bar"
            assert b"foo" in shelf
            assert b"baz" not in shelf
            assert shelf.get(b'foo') == b'bar'
            assert shelf.get(b'baz', None) is None
        finally:
            shelf._db.shutdown()

    def test_pop(self, base_dir):
        """Test that .pop() works."""
        path = os.path.join(base_dir, 'shelf_pop')
        shelf = TritcaskShelf(0, Tritcask(path))
        try:
            shelf[b"foo"] = b"bar"
            assert shelf.pop(b"foo") == b"bar"
            assert b"foo" not in shelf
            # bad key
            with pytest.raises(KeyError):
                shelf.pop(b"no-key")
        finally:
            shelf._db.shutdown()

    def test_get(self, base_dir):
        """Test that it behaves with the .get(key, default)."""
        path = os.path.join(base_dir, 'shelf_get')
        shelf = TritcaskShelf(0, Tritcask(path))
        try:
            shelf[b"foo"] = b"bar"
            assert shelf.get(b'foo') == b'bar'
            assert shelf.get(b'foo', None) == b'bar'
            assert shelf.get(b'baz') is None
            assert shelf.get(b'baz', False) is False
        finally:
            shelf._db.shutdown()

    def test_items(self, base_dir):
        """Test the items method."""
        path = os.path.join(base_dir, 'shelf_items')
        shelf = TritcaskShelf(0, Tritcask(path))
        try:
            shelf[b"foo"] = b"bar"
            assert list(shelf.items()) == [(b'foo', b'bar')]
            shelf[b"foo1"] = b"bar1"
            assert (b'foo', b'bar') in shelf.items()
            assert (b'foo1', b'bar1') in shelf.items()
        finally:
            shelf._db.shutdown()

    def test_custom_serialization(self, base_dir):
        """Test the _serialize and _deserialize methods."""
        path = os.path.join(base_dir, 'shelf_serialization')

        class MarshalShelf(TritcaskShelf):
            """A shelf that use marshal for de/serialization."""

            def _deserialize(self, value):
                """Custom _serialize."""
                return marshal.loads(value)

            def _serialize(self, value):
                """Custom _deserialize."""
                return marshal.dumps(value)

        db = Tritcask(path)
        try:
            shelf = MarshalShelf(0, db, serialize_keys=False)
            shelf[b'foo'] = b'bar'
            assert b'bar' in shelf[b'foo']
            assert db.get(0, b'foo') == marshal.dumps(b'bar')
        finally:
            db.shutdown()

    def test_keys(self, base_dir):
        """Test for the keys method."""
        path = os.path.join(base_dir, 'shelf_get')
        shelf = TritcaskShelf(0, Tritcask(path))
        try:
            for i in range(10):
                shelf[('foo%d' % (i,)).encode('ascii')] = ('bar%d' % (i,)).encode('ascii')
            keys = shelf.keys()
            assert type(keys) is types.GeneratorType
            assert len(list(keys)) == 10
        finally:
            shelf._db.shutdown()

    def test__len__(self, base_dir):
        """Test for the __len__ method."""
        path = os.path.join(base_dir, 'shelf_len')
        shelf = TritcaskShelf(0, Tritcask(path))
        try:
            for i in range(10):
                shelf[('foo%d' % (i,)).encode('ascii')] = ('bar%d' % (i,)).encode('ascii')
            assert len(shelf) == 10
            shelf[b'foo_a'] = b'bar_a'
            assert len(shelf) == 11
        finally:
            shelf._db.shutdown()

    def test_has_key(self, base_dir):
        """Test for has_key."""
        path = os.path.join(base_dir, 'shelf_has_key')
        shelf = TritcaskShelf(0, Tritcask(path))
        try:
            shelf['foo0'.encode('ascii')] = 'bar0'.encode('ascii')
            assert b'foo0' in shelf
        finally:
            shelf._db.shutdown()


# ============================================================================
# WindowsTimerTests
# ============================================================================

class TestWindowsTimer:
    """Tests for the windows timer."""

    def test_initial_value(self):
        """Test that the initial value is > 0."""
        timer = WindowsTimer()
        assert int(timer.time()) > 0


# ============================================================================
# TestCaskSerialization
# ============================================================================

class TestCaskSerialization:
    """Tests for the Cask class (JSON-serialized KV store)."""

    def test_all_json_primitives_as_keys_and_values(self, base_dir):
        """Test all JSON primitives as keys and values."""
        cask = Cask.from_path(base_dir)
        primitives = [
            None,
            True,
            False,
            123,
            4.56,
            "hello",
            [1, 2, "abc", None, False],
            {"z": 1, "t": [2, 3], "b": False, "c": "hello", "d": {"e": 1.23}}
        ]
        for val in primitives:
            cask[val] = val
            assert cask[val] == val

    def test_keys_consistency(self, base_dir):
        """Test keys consistency."""
        cask = Cask.from_path(base_dir)
        # Ensure that keys() yields all keys, and __len__ matches count
        keys_inserted = [{"a": i, "b": [i, i+1]} for i in range(5)]
        for i, k in enumerate(keys_inserted):
            cask[k] = i
        listed = list(cask.keys())
        for k in keys_inserted:
            found = any(dict(k) == dict(item) if isinstance(item, dict) else False for item in listed)
            assert found
        assert len(cask) == len(keys_inserted)

        # Different order dict keys
        k1 = {"x": 7, "y": 8}
        k2 = {"y": 8, "x": 7}
        cask[k1] = 42
        assert k2 in cask
        assert cask[k2] == 42
        assert cask[k1] == 42

        # Overwrite one with the other order, value should change
        cask[k2] = 99
        assert cask[k1] == 99
        assert cask[k2] == 99

        # Length should not increase
        before = len(cask)
        cask[k1] = 123
        assert len(cask) == before

        # keys() generator type
        assert isinstance(cask.keys(), types.GeneratorType)
        assert any(isinstance(k, dict) for k in cask.keys())

