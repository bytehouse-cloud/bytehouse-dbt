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
from dbt.exceptions import CompilationException
from dbt.tests.adapter.utils.fixture_listagg import (
    models__test_listagg_yml,
    seeds__data_listagg_csv,
)
from dbt.tests.util import run_dbt

models__test_listagg_sql = """
  select
  group_col,
  {{listagg('string_text', "'_|_'", "order by order_col")}} as actual,
  'bottom_ordered' as version
from {{ ref('data_listagg') }} group by group_col
"""


class TestListagg:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_listagg.csv": seeds__data_listagg_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_listagg.yaml": models__test_listagg_yml,
            "test_listagg.sql": models__test_listagg_sql,
        }

    def test_listagg_exception(self, project):
        try:
            run_dbt(["build"], False)
        except CompilationException as e:
            assert 'does not support' in e.msg
