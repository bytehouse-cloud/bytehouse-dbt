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

import bytehouse_driver
from dateutil import parser
from bytehouse_driver import Client
from bytehouse_driver.errors import NetworkError, SocketTimeoutError
from dbt.exceptions import DatabaseException as DBTDatabaseException
from dbt.version import __version__ as dbt_version

from dbt.adapters.bytehouse import ByteHouseCredentials
from dbt.adapters.bytehouse.dbclient import BhClientWrapper, BhRetryableException


class BhNativeClient(BhClientWrapper):
    def query(self, sql, **kwargs):
        self.prepare_system_database(sql)
        sql = self.rewrite_sql(sql)
        if "rename table" in sql:
            sql = self.get_rename_view_sql(sql)
        if "drop table" in sql or "DROP TABLE" in sql:
            sql = self.modify_drop_table_syntax(sql)
        try:
            return NativeClientResult(self._client.execute(sql, with_column_types=True, **kwargs))
        except bytehouse_driver.errors.Error as ex:
            raise DBTDatabaseException(str(ex).strip()) from ex

    def command(self, sql, **kwargs):
        self.prepare_system_database(sql)
        sql = self.rewrite_sql(sql)
        if "rename table" in sql:
            sql = self.get_rename_view_sql(sql)
        if "drop table" in sql or "DROP TABLE" in sql:
            sql = self.modify_drop_table_syntax(sql)
        try:
            # TODO: Check whether Open source clickhouse support insertion without data block
            if "ByteHouseCustomCSV" in sql:
                query, data = self.handle_format_csv(sql)
                result = self._client.execute(query, data, with_column_types=True, **kwargs)
                return result
            result = self._client.execute(sql, **kwargs)
            if len(result) and len(result[0]):
                return result[0][0]
        except bytehouse_driver.errors.Error as ex:
            raise DBTDatabaseException(str(ex).strip()) from ex

    def close(self):
        self._client.disconnect()

    def _create_client(self, credentials: ByteHouseCredentials):
        client = bytehouse_driver.Client(
            host=credentials.host,
            port=credentials.port,
            region=credentials.region,
            account=credentials.account,
            user=credentials.user,
            password=credentials.password,
            client_name=f'dbt-{dbt_version}',
            secure=credentials.secure,
            verify=credentials.verify,
            connect_timeout=credentials.connect_timeout,
            send_receive_timeout=credentials.send_receive_timeout,
            sync_request_timeout=credentials.sync_request_timeout,
            compress_block_size=credentials.compress_block_size,
            compression=False if credentials.compression == '' else credentials.compression,
            settings=self._conn_settings,
        )
        try:
            client.connection.connect()
            if credentials.warehouse is not None and credentials.warehouse != "":
                if self._is_warehouse_up(client, credentials.warehouse) is False:
                    client.execute('resume warehouse {}'.format(credentials.warehouse))
                client.execute('set warehouse {}'.format(credentials.warehouse))
        except (SocketTimeoutError, NetworkError) as ex:
            raise BhRetryableException(str(ex)) from ex
        return client

    def _is_warehouse_up(self, client: Client, vw_name):
        warehouses = client.execute("show warehouses")
        for warehouse in warehouses:
            if warehouse[1] == vw_name and warehouse[6] == "up":
                return True
        return False

    def _set_client_database(self):
        # After we know the database exists, reconnect to that database if appropriate
        if self._client.connection.database != self.database:
            self._client.connection.disconnect()
            self._client.connection.database = self.database
            self._client.connection.connect()

    def _server_version(self):
        server_info = self._client.connection.server_info
        return (
            f'{server_info.version_major}.{server_info.version_minor}.{server_info.version_patch}'
        )

    def prepare_system_database(self, sql):
        if "system_meta" not in sql:
            return
        # TODO: Add log & exception handling
        self._client.execute("DROP DATABASE IF EXISTS system_meta")
        self._client.execute("CREATE DATABASE system_meta")
        self._client.execute("CREATE TABLE system_meta.databases (name String, engine String, comment String) engine = CnchMergeTree("
                             ") order by tuple()")
        databases_result = self._client.execute("SHOW DATABASES")
        databases_holder = []
        for database in databases_result:
            database_name = database[0]
            if database_name.startswith("dbt"):
                databases_holder.append(database)
        databases_result = databases_holder
        if len(databases_result) > 0:
            self._client.execute("INSERT INTO system_meta.databases VALUES", ((x[0], x[8], x[7]) for x in databases_result))

        if "system_meta.tables" not in sql and "system_meta.columns" not in sql:
            return

        self._client.execute("CREATE TABLE system_meta.tables (name String, database String, engine String, comment "
                             "String, type String) engine = CnchMergeTree() order by tuple()")

        table_names = []
        for database in databases_result:
            database_name = database[0]
            tables_result = self._client.execute(f"SHOW TABLES FROM {database_name}")
            for table in tables_result:
                table_name = table[0]
                table_names.append(f'{database_name}.{table_name}')
            if len(tables_result) > 0:
                self._client.execute("INSERT INTO system_meta.tables VALUES",
                                     ((x[0], database_name, "CnchMergeTree", x[7], x[8]) for x in tables_result))

        if "system_meta.columns" not in sql:
            return

        self._client.execute("CREATE TABLE system_meta.columns (database String, table String, name String, position "
                             "Int, type String, comment String) engine = CnchMergeTree() order by tuple()")
        for table_name in table_names:
            database_identifier = table_name.split(".")[0]
            table_identifier = table_name.split(".")[1]
            columns_result = self._client.execute(f"DESCRIBE TABLE {table_name}")
            # TODO: Verify ordering is the same as index
            columns_data = []
            idx = 0
            for x in columns_result:
                columns_data.append([database_identifier, table_identifier, x[0], idx, x[1], x[4]])
                idx = idx + 1
            if len(columns_result) > 0:
                self._client.execute("INSERT INTO system_meta.columns VALUES", columns_data)

    def rewrite_sql(self, sql):
        # TODO: Engine rewrite from upstream caller
        sql = sql.replace("MergeTree", "CnchMergeTree")
        # TODO: SETTINGS rewrite from upstream caller
        sql = sql.replace("SETTINGS  allow_nullable_key=1", "")
        return sql

    def get_rename_view_sql(self, sql):
        sql = self.modify_rename_table_syntax(sql)
        tokens = sql.split(" ")
        old_identifier = tokens[2]
        new_identifier = tokens[4]
        if not self.is_view(old_identifier):
            return sql
        old_view_create_table = self._client.execute(f'SHOW CREATE TABLE {old_identifier}')[0][0]
        tokens = old_view_create_table.split(" ")
        tokens[2] = new_identifier
        new_view_create_table = ' '.join(tokens)

        self._client.execute(f'DROP VIEW IF EXISTS {old_identifier}')
        return new_view_create_table

    def modify_drop_table_syntax(self, sql):
        tokens = sql.split(" ")
        table_identifier = ""
        for iter in range(0, len(tokens)-1):
            if tokens[iter].upper().endswith("DROP") and tokens[iter+1].upper() == "TABLE":
                table_identifier = tokens[iter+4]
                break
        if not self.is_view(table_identifier):
            return sql
        for iter in range(0, len(tokens)-1):
            if tokens[iter].upper().endswith("DROP") and tokens[iter+1].upper() == "TABLE":
                tokens[iter+1] = "view"
                break
        return ' '.join(tokens)

    def is_view(self, identifier):
        tokens = identifier.split(".")
        database_name = tokens[0]
        table_name = tokens[1]

        table_results = self._client.execute(f'SHOW TABLES FROM {database_name}')
        for table in table_results:
            name = table[0]
            table_type = table[8]
            if name == table_name and table_type == "VIEW":
                return True
        return False

    def modify_rename_table_syntax(self, sql):
        tokens = sql.split(" ")
        modify_sql = ""
        st = False
        for token in tokens:
            if token == "rename":
                st = True
            if st:
                modify_sql += token
                modify_sql += " "
        return modify_sql

    def handle_format_csv(self, sql):
        tokens = sql.split("|")

        insert_query = tokens[1]
        column_count = int(tokens[3])

        datatypes = []
        for i in range(4, 4 + column_count):
            datatypes.append(tokens[i])

        # TODO: Support all datatypes
        data = []
        for i in range(4 + column_count, len(tokens), column_count):
            row = []
            idx = 0
            for j in range(i, i + column_count):
                val = tokens[j]
                if datatypes[idx] == "String":
                    val = str(val)
                if datatypes[idx] == "Datetime":
                    val = parser.parse(val)
                if datatypes[idx] == "Int":
                    if str(val) == 'None':
                        val = 0
                    val = int(val)
                if datatypes[idx] == "BooleanInt":
                    cur = str(val).upper()
                    if cur == "TRUE":
                        val = 1
                    else:
                        val = 0
                row.append(val)
                idx += 1
            data.append(row)
        return insert_query, data


class NativeClientResult:
    def __init__(self, native_result):
        self.result_set = native_result[0]
        self.column_names = [col[0] for col in native_result[1]]