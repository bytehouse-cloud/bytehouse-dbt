"""
   Copyright 2016-2022 ClickHouse, Inc.

   Copyright 2022- 2023 Bytedance Ltd. and/or its affiliates

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

#!/usr/bin/env python

import os
import re

from setuptools import find_namespace_packages, setup

this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md')) as f:
    long_description = f.read()


# get this from a separate file
def _dbt_bytehouse_version():
    _version_path = os.path.join(this_directory, 'dbt', 'adapters', 'bytehouse', '__version__.py')
    _version_pattern = r'''version\s*=\s*["'](.+)["']'''
    with open(_version_path) as f:
        match = re.search(_version_pattern, f.read().strip())
        if match is None:
            raise ValueError(f'invalid version at {_version_path}')
        return match.group(1)


package_name = 'dbt-bytehouse'
package_version = _dbt_bytehouse_version()
description = '''The ByteHouse plugin for dbt (data build tool)'''

dbt_version = '1.3.0'
dbt_minor = '.'.join(dbt_version.split('.')[0:2])

if not package_version.startswith(dbt_minor):
    raise ValueError(
        f'Invalid setup.py: package_version={package_version} must start with '
        f'dbt_version={dbt_minor}'
    )

github_url = 'https://github.com/bytehouse-cloud/bytehouse-dbt'

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Rafsan Mazumder',
    author_email='rafsan.mazumder@bytedance.com',
    url=github_url,
    license='MIT',
    keywords='ByteHouse dbt connector',
    project_urls={
        'Documentation': github_url,
        'Changes': github_url + '/blob/main/CHANGELOG.md'
    },
    packages=find_namespace_packages(include=['dbt', 'dbt.*']),
    package_data={
        'dbt': [
            'include/bytehouse/dbt_project.yml',
            'include/bytehouse/macros/*.sql',
            'include/bytehouse/macros/**/*.sql',
        ]
    },
    install_requires=[
        f'dbt-core~={dbt_version}',
        'bytehouse-driver',
        'python-dateutil',
    ],
    python_requires=">=3.7",
    platforms='any',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
)
