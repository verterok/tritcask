#!/usr/bin/env python
#
# Author: Guillermo Gonzalez <guillermo.gonzalez@canonical.com>
#
# Copyright 2012 Canonical Ltd.
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
from distutils.core import setup

setup(name='Tritcask',
      version='0.2',
      description='Append only (type,key)/value store based on bitcask paper.',
      long_description=open("README.md").read(),
      author='Guillermo Gonzalez',
      author_email='guillermo.gonzalez@canonical.com',
      url='https://github.com/verterok/tritcask',
      license='GNU GPL v3',
      packages=['tritcask'],
      provides=['tritcask'],
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: GNU General Public License (GPL)',
          'Operating System :: OS Independent',
          'Operating System :: POSIX',
          'Programming Language :: Python',
          'Topic :: Database',
      ],
     )
