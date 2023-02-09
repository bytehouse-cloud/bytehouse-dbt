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

import csv
import io
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Set, Union

import agate
import dbt.exceptions
from dbt.adapters.base import AdapterConfig, available
from dbt.adapters.base.impl import catch_as_completed
from dbt.adapters.base.relation import BaseRelation, InformationSchema
from dbt.adapters.sql import SQLAdapter
from dbt.clients.agate_helper import table_from_rows
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.relation import RelationType
from dbt.events import AdapterLogger
from dbt.utils import executor, filter_null_values

from dbt.adapters.bytehouse.column import ByteHouseColumn
from dbt.adapters.bytehouse.connections import ByteHouseConnectionManager
from dbt.adapters.bytehouse.relation import ByteHouseRelation

GET_CATALOG_MACRO_NAME = 'get_catalog'
LIST_SCHEMAS_MACRO_NAME = 'list_schemas'


@dataclass
class ByteHouseConfig(AdapterConfig):
    engine: str = 'MergeTree()'
    order_by: Optional[Union[List[str], str]] = 'tuple()'
    partition_by: Optional[Union[List[str], str]] = None


class ByteHouseAdapter(SQLAdapter):
    Relation = ByteHouseRelation
    Column = ByteHouseColumn
    ConnectionManager = ByteHouseConnectionManager
    AdapterSpecificConfigs = ByteHouseConfig
    logger = AdapterLogger("dbt_bytehouse_tests")

    @classmethod
    def date_function(cls):
        return 'now()'

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return 'String'

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        # We match these type to the Column.TYPE_LABELS for consistency
        return 'Float32' if decimals else 'Int32'

    @classmethod
    def convert_boolean_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return 'Int32'

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return 'DateTime'

    @classmethod
    def convert_date_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return 'Date'

    @classmethod
    def convert_time_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        raise dbt.exceptions.NotImplementedException(
            '`convert_time_type` is not implemented for this adapter!'
        )

    @available.parse(lambda *a, **k: {})
    def get_bytehouse_cluster_name(self):
        conn = self.connections.get_if_exists()
        if conn.credentials.cluster:
            return '"{}"'.format(conn.credentials.cluster)

    @available
    def bytehouse_db_engine_clause(self):
        conn = self.connections.get_if_exists()
        if conn and conn.credentials.database_engine:
            return f'ENGINE {conn.credentials.database_engine}'
        return ''

    @available
    def is_before_version(self, version: str) -> bool:
        conn = self.connections.get_if_exists()
        if conn:
            server_version = conn.handle.server_version
            return compare_versions(version, server_version) > 0
        return False

    @available
    def supports_atomic_exchange(self) -> bool:
        conn = self.connections.get_if_exists()
        return conn and conn.handle.atomic_exchange

    @available
    def can_exchange(self, schema: str, rel_type: str) -> bool:
        if rel_type != 'table' or not schema or not self.supports_atomic_exchange():
            return False
        ch_db = self.get_ch_database(schema)
        return ch_db and ch_db.engine in ('Atomic', 'Replicated')

    def check_schema_exists(self, database, schema):
        results = self.execute_macro(LIST_SCHEMAS_MACRO_NAME, kwargs={'database': database})
        return schema in (row[0] for row in results)

    def drop_schema(self, relation: BaseRelation) -> None:
        super().drop_schema(relation)
        conn = self.connections.get_if_exists()
        if conn:
            conn.handle.database_dropped(relation.schema)

    def _make_match_kwargs(self, database: str, schema: str, identifier: str) -> Dict[str, str]:
        return filter_null_values(
            {
                "database": database,
                "identifier": identifier,
            }
        )

    def list_relations_without_caching(
        self, schema_relation: ByteHouseRelation
    ) -> List[ByteHouseRelation]:
        kwargs = {'schema_relation': schema_relation}
        results = self.execute_macro('list_relations_without_caching', kwargs=kwargs)
        conn_supports_exchange = self.supports_atomic_exchange()

        relations = []
        for row in results:
            name, schema, type_info, db_engine = row
            rel_type = RelationType.View if 'view' in type_info else RelationType.Table
            can_exchange = (
                conn_supports_exchange
                and rel_type == RelationType.Table
                and db_engine in ('Atomic', 'Replicated')
            )
            relation = self.Relation.create(
                database=None,
                schema=schema,
                identifier=name,
                type=rel_type,
                can_exchange=can_exchange,
            )
            relations.append(relation)

        return relations

    def get_relation(self, database: Optional[str], schema: str, identifier: str):
        if not self.Relation.include_policy.database:
            database = None

        return super().get_relation(database, schema, identifier)

    @available
    def get_ch_database(self, schema: str):
        try:
            results = self.execute_macro('bytehouse__get_database', kwargs={'database': schema})
            if len(results.rows):
                return ByteHouseDatabase(**results.rows[0])
            return None
        except dbt.exceptions.RuntimeException:
            return None

    def parse_bytehouse_columns(
        self, relation: ByteHouseRelation, raw_rows: List[agate.Row]
    ) -> List[ByteHouseColumn]:
        rows = [dict(zip(row._keys, row._values)) for row in raw_rows]

        return [
            ByteHouseColumn(
                column=column['name'],
                dtype=column['type'],
            )
            for column in rows
        ]

    def get_columns_in_relation(self, relation: ByteHouseRelation) -> List[ByteHouseColumn]:
        rows: List[agate.Row] = super().get_columns_in_relation(relation)
        return self.parse_bytehouse_columns(relation, rows)

    def get_catalog(self, manifest):
        schema_map = self._get_catalog_schemas(manifest)

        with executor(self.config) as tpe:
            futures: List[Future[agate.Table]] = []
            for info, schemas in schema_map.items():
                for schema in schemas:
                    futures.append(
                        tpe.submit_connected(
                            self,
                            schema,
                            self._get_one_catalog,
                            info,
                            [schema],
                            manifest,
                        )
                    )
            catalogs, exceptions = catch_as_completed(futures)
        return catalogs, exceptions

    def _get_one_catalog(
        self,
        information_schema: InformationSchema,
        schemas: Set[str],
        manifest: Manifest,
    ) -> agate.Table:
        if len(schemas) != 1:
            dbt.exceptions.raise_compiler_error(
                f'Expected only one schema in bytehouse _get_one_catalog, found ' f'{schemas}'
            )

        return super()._get_one_catalog(information_schema, schemas, manifest)

    @classmethod
    def _catalog_filter_table(cls, table: agate.Table, manifest: Manifest) -> agate.Table:
        table = table_from_rows(
            table.rows,
            table.column_names,
            text_only_columns=['table_schema', 'table_name'],
        )
        return table.where(_catalog_filter_schemas(manifest))

    def get_rows_different_sql(
        self,
        relation_a: ByteHouseRelation,
        relation_b: ByteHouseRelation,
        column_names: Optional[List[str]] = None,
        except_operator: str = None,
    ) -> str:
        names: List[str]
        if column_names is None:
            columns = self.get_columns_in_relation(relation_a)
            names = sorted((self.quote(c.name) for c in columns))
        else:
            names = sorted((self.quote(n) for n in column_names))

        alias_a = 'ta'
        alias_b = 'tb'
        columns_csv_a = ', '.join([f'{alias_a}.{name}' for name in names])
        columns_csv_b = ', '.join([f'{alias_b}.{name}' for name in names])
        join_condition = ' AND '.join([f'{alias_a}.{name} = {alias_b}.{name}' for name in names])
        first_column = names[0]

        # ByteHouse doesn't have an EXCEPT operator
        sql = COLUMNS_EQUAL_SQL.format(
            alias_a=alias_a,
            alias_b=alias_b,
            first_column=first_column,
            columns_a=columns_csv_a,
            columns_b=columns_csv_b,
            join_condition=join_condition,
            relation_a=str(relation_a),
            relation_b=str(relation_b),
        )

        return sql

    def update_column_sql(
        self,
        dst_name: str,
        dst_column: str,
        clause: str,
        where_clause: Optional[str] = None,
    ) -> str:
        clause = f'alter table {dst_name} update {dst_column} = {clause}'
        if where_clause is not None:
            clause += f' where {where_clause}'
        return clause

    @available
    def get_csv_data(self, table):
        # TODO: Consider passing table to macro
        row_count = len(table.rows)
        column_count = 0
        if row_count > 0:
            column_count = len(table.rows[0])

        # TODO: Use hashmap for conversion
        # TODO: Implement all datatypes
        datatypes = []
        for c in table._column_types:
            if isinstance(c, dbt.clients.agate_helper.Number):
                datatypes.append("Int")
            elif isinstance(c, dbt.clients.agate_helper.ISODateTime):
                datatypes.append("Datetime")
            elif isinstance(c, agate.data_types.boolean.Boolean):
                datatypes.append("BooleanInt")
            elif isinstance(c, agate.data_types.date_time.DateTime):
                datatypes.append("Datetime")
            elif isinstance(c, agate.data_types.number.Number):
                datatypes.append("Int")
            elif isinstance(c, agate.data_types.text.Text):
                datatypes.append("String")
            else:
                datatypes.append("NotImplemented")

        data = "|" + str(row_count)
        data += "|" + str(column_count)
        for datatype in datatypes:
            data += "|" + datatype
        for row in table.rows:
            for datatype, val in enumerate(row):
                data += "|" + str(val)
        return data

    def run_sql_for_tests(self, sql, fetch, conn):
        client = conn.handle
        try:
            if fetch:
                result = client.query(sql).result_set
            else:
                result = client.command(sql)
            if fetch == "one" and len(result) > 0:
                return result[0]
            if fetch == "all":
                return result
        except BaseException as e:
            self.logger.error(sql)
            self.logger.error(e)
            raise
        finally:
            conn.state = 'close'

    @available
    def get_model_settings(self, model):
        settings = model['config'].get('settings', dict())
        res = []
        for key in settings:
            res.append(f' {key}={settings[key]}')
        return '' if len(res) == 0 else 'SETTINGS ' + ', '.join(res) + '\n'


@dataclass
class ByteHouseDatabase:
    name: str
    engine: str
    comment: str


def _expect_row_value(key: str, row: agate.Row):
    if key not in row.keys():
        raise dbt.exceptions.InternalException(
            f'Got a row without \'{key}\' column, columns: {row.keys()}'
        )

    return row[key]


def _catalog_filter_schemas(manifest: Manifest) -> Callable[[agate.Row], bool]:
    schemas = frozenset((None, s.lower()) for d, s in manifest.get_used_schemas())

    def test(row: agate.Row) -> bool:
        table_database = _expect_row_value('table_database', row)
        table_schema = _expect_row_value('table_schema', row)
        if table_schema is None:
            return False
        return (table_database, table_schema.lower()) in schemas

    return test


def compare_versions(v1: str, v2: str) -> int:
    v1_parts = v1.split('.')
    v2_parts = v2.split('.')
    for part1, part2 in zip(v1_parts, v2_parts):
        try:
            if int(part1) != int(part2):
                return 1 if int(part1) > int(part2) else -1
        except ValueError:
            raise dbt.exceptions.RuntimeException(
                "Version must consist of only numbers separated by '.'"
            )
    # Versions are equal - return False
    return 0


COLUMNS_EQUAL_SQL = '''
SELECT
    row_count_diff.difference as row_count_difference,
    diff_count.num_missing as num_mismatched
FROM (
    SELECT
        1 as id,
        (SELECT COUNT(*) as num_rows FROM {relation_a}) -
        (SELECT COUNT(*) as num_rows FROM {relation_b}) as difference
    ) as row_count_diff
INNER JOIN (
    SELECT
        1 as id,
        COUNT(*) as num_missing FROM (
            SELECT
                {columns_a}
            FROM {relation_a} as {alias_a}
            LEFT OUTER JOIN {relation_b} as {alias_b}
                ON {join_condition}
            WHERE {alias_b}.{first_column} IS NULL
            UNION ALL
            SELECT
                {columns_b}
            FROM {relation_b} as {alias_b}
            LEFT OUTER JOIN {relation_a} as {alias_a}
                ON {join_condition}
            WHERE {alias_a}.{first_column} IS NULL
        ) as missing
    ) as diff_count ON row_count_diff.id = diff_count.id
'''.strip()
