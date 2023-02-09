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

import json

import pytest
from dbt.tests.util import run_dbt

ref_models__table_comment_sql = """
{{
  config(
    materialized = "table",
    persist_docs = {"relation": true, "columns": true},
  )
}}

select
    'foo' as first_name,
    'bar' as second_name

"""

ref_models__view_comment_sql = """
{{
  config(
    materialized = "view",
    persist_docs = {"relation": true, "columns": true},
  )
}}

select
    'foo' as first_name,
    'bar' as second_name

"""

ref_models__schema_yml = """
version: 2

models:
  - name: table_comment
    description: "YYY table"
    columns:
      - name: first_name
        description: "XXX first description"
      - name: second_name
        description: "XXX second description"
  - name: view_comment
    description: "YYY view"
    columns:
      - name: first_name
        description: "XXX first description"
      - name: second_name
        description: "XXX second description"
"""


class TestBaseComment:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": ref_models__schema_yml,
            "table_comment.sql": ref_models__table_comment_sql,
            "view_comment.sql": ref_models__view_comment_sql,
        }

    @pytest.mark.parametrize(
        'model_name',
        ['table_comment', 'view_comment'],
    )
    def test_comment(self, project, model_name):
        run_dbt(["run"])
        run_dbt(["docs", "generate"])
        with open("target/catalog.json") as fp:
            catalog_data = json.load(fp)

        assert "nodes" in catalog_data
        column_node = catalog_data["nodes"][f"model.test.{model_name}"]
        for column in column_node["columns"].keys():
            column_comment = column_node["columns"][column]["comment"]
            assert column_comment.startswith("XXX")

        assert column_node['metadata']['comment'].startswith("YYY")
