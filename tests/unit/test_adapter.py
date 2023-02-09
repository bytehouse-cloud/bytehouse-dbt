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

from dbt.adapters.bytehouse.impl import compare_versions


def test_is_before_version():
    assert compare_versions('20.0.0', '21.0.0') == -1
    assert compare_versions('20.1.0', '21.0.0') == -1
    assert compare_versions('20.1.1', '21.0.0') == -1
    assert compare_versions('20.0.0', '21.0') == -1
    assert compare_versions('21.0.0', '21.0.0') == 0
    assert compare_versions('21.1.0', '21.0.0') == 1
    assert compare_versions('22.0.0', '21.0.0') == 1
    assert compare_versions('21.0.1', '21.0.0') == 1
    assert compare_versions('21.0.1', '21.0') == 0
