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

import re
import time
from contextlib import contextmanager
from typing import Any, Optional, Tuple

import agate
import dbt.exceptions
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import Connection
from dbt.events import AdapterLogger

from dbt.adapters.bytehouse.dbclient import BhRetryableException, get_db_client

logger = AdapterLogger('bytehouse')
retryable_exceptions = [BhRetryableException]
ddl_re = re.compile(r'^\s*(CREATE|DROP|ALTER)\s', re.IGNORECASE)


class ByteHouseConnectionManager(SQLConnectionManager):
    """
    ByteHouse Connector connection manager.
    """

    TYPE = 'bytehouse'

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield
        except Exception as exp:
            logger.debug('Error running SQL: {}', sql)
            if isinstance(exp, dbt.exceptions.RuntimeException):
                raise
            raise dbt.exceptions.RuntimeException(exp) from exp

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection
        credentials = cls.get_credentials(connection.credentials)

        def connect():
            return get_db_client(credentials)

        return cls.retry_connection(
            connection,
            connect=connect,
            logger=logger,
            retry_limit=credentials.retries,
            retryable_exceptions=retryable_exceptions,
        )

    def cancel(self, connection):
        connection_name = connection.name
        logger.debug('Cancelling query \'{}\'', connection_name)
        connection.handle.close()
        logger.debug('Cancel query \'{}\'', connection_name)

    def release(self):
        pass  # There is no "release" type functionality in the existing ByteHouse connectors

    @classmethod
    def get_table_from_response(cls, response, column_names) -> agate.Table:
        """
        Build agate table from response.
        :param response: ByteHouse query result
        :param column_names: Table column names
        """
        data = []
        for row in response:
            data.append(dict(zip(column_names, row)))

        return dbt.clients.agate_helper.table_from_data_flat(data, column_names)

    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False
    ) -> Tuple[str, agate.Table]:
        # Don't try to fetch result of clustered DDL responses, we don't know what to do with them
        if fetch and ddl_re.match(sql):
            fetch = False

        sql = self._add_query_comment(sql)
        conn = self.get_thread_connection()
        client = conn.handle

        with self.exception_handler(sql):
            logger.debug(f'On {conn.name}: {sql}...')
            pre = time.time()
            if fetch:
                query_result = client.query(sql)
            else:
                query_result = client.command(sql)
            status = self.get_status(client)
            logger.debug(f'SQL status: {status} in {(time.time() - pre):.2f} seconds')
            if fetch:
                table = self.get_table_from_response(
                    query_result.result_set, query_result.column_names
                )
            else:
                table = dbt.clients.agate_helper.empty_table()
            return status, table

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
    ) -> Tuple[Connection, Any]:
        sql = self._add_query_comment(sql)
        conn = self.get_thread_connection()
        client = conn.handle

        with self.exception_handler(sql):
            logger.debug(f'On {conn.name}: {sql}...')

            pre = time.time()
            client.command(sql)

            status = self.get_status(client)

            logger.debug(f'SQL status: {status} in {(time.time() - pre):0.2f} seconds')

            return conn, None

    @classmethod
    def get_credentials(cls, credentials):
        """
        Returns ByteHouse credentials
        """
        return credentials

    @classmethod
    def get_status(cls, _):
        """
        Returns connection status
        """
        return 'OK'

    @classmethod
    def get_response(cls, _):
        return 'OK'

    def begin(self):
        pass

    def commit(self):
        pass
