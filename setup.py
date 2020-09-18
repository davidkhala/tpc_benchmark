"""Setup of project for packaging

MIT License, see LICENSE file for complete text.
Copyright (c) 2020 SADA Systems, Inc.
"""
from setuptools import setup

setup(name='tpc_benchmark',
      version='0.1',
      description='TPC-DS & TPC-H testing',
      url='https://github.com/sadasystems/tpc_benchmark',
      author='Colin Dietrich',
      author_email='',
      license='MIT',
      packages=['tpc_benchmark'],
      zip_safe=False)