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
from dbt.tests.adapter.utils.base_array_utils import BaseArrayUtils
from dbt.tests.adapter.utils.fixture_array_append import models__array_append_actual_sql
from dbt.tests.adapter.utils.fixture_array_concat import models__array_concat_actual_sql

# Empty arrays are constructed with the DBT default "integer" which is an Int32.  Because ClickHouse will coerce
# the arrays to the smallest possible type, we need to ensure that at least one of the members requires an Int32
models__array_append_expected_sql = """
select 1 as id, {{ array_construct([1,2,3,-77777777]) }} as array_col union all
select 2 as id, {{ array_construct([4]) }} as array_col
"""


class TestArrayAppend(BaseArrayUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "actual.sql": models__array_append_actual_sql,
            "expected.sql": models__array_append_expected_sql,
        }


models__array_concat_expected_sql = """
select 1 as id, {{ array_construct([1,2,3,4,5,-77777777]) }} as array_col union all
select 2 as id, {{ array_construct([2]) }} as array_col union all
select 3 as id, {{ array_construct([3]) }} as array_col
"""


class TestArrayConcat(BaseArrayUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "actual.sql": models__array_concat_actual_sql,
            "expected.sql": models__array_concat_expected_sql,
        }
