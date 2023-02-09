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

import pytest
from dbt.tests.adapter.utils.fixture_split_part import models__test_split_part_yml
from dbt.tests.adapter.utils.test_split_part import BaseSplitPart

models__test_split_part_sql = """
with data as (

    select * from {{ ref('data_split_part') }}

)

select
    {{ split_part('parts', '|', 1) }} as actual,
    result_1 as expected

from data

union all

select
    {{ split_part('parts', '|', 2) }} as actual,
    result_2 as expected

from data

union all

select
    {{ split_part('parts', '|', 3) }} as actual,
    result_3 as expected

from data
"""


class TestSplitPart(BaseSplitPart):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_split_part.yml": models__test_split_part_yml,
            "test_split_part.sql": models__test_split_part_sql,
        }
