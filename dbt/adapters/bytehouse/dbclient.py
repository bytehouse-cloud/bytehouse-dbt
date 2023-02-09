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

from abc import ABC, abstractmethod

from dbt.events import AdapterLogger
from dbt.exceptions import FailedToConnectException

from dbt.adapters.bytehouse.credentials import ByteHouseCredentials

logger = AdapterLogger('bytehouse')


def get_db_client(credentials: ByteHouseCredentials):
    credentials.driver = 'native'
    try:
        import bytehouse_driver  # noqa

        from dbt.adapters.bytehouse.nativeclient import BhNativeClient

        return BhNativeClient(credentials)
    except ImportError:
        raise FailedToConnectException(
            'Native adapter required but package bytehouse-driver is not installed'
        )


class BhRetryableException(Exception):
    pass


class BhClientWrapper(ABC):
    def __init__(self, credentials: ByteHouseCredentials):
        self.database = credentials.schema
        self._conn_settings = credentials.custom_settings or {}
        if credentials.cluster_mode or credentials.database_engine == 'Replicated':
            self._conn_settings['database_replicated_enforce_synchronous_settings'] = '1'
            self._conn_settings['insert_quorum'] = 'auto'
        self._client = self._create_client(credentials)
        check_exchange = credentials.check_exchange and not credentials.cluster_mode
        try:
            self._ensure_database(credentials.database_engine)
            self.server_version = self._server_version()
            self.atomic_exchange = not check_exchange or self._check_atomic_exchange()
        except Exception as ex:
            self.close()
            raise ex

    @abstractmethod
    def query(self, sql: str, **kwargs):
        pass

    @abstractmethod
    def command(self, sql: str, **kwargs):
        pass

    def database_dropped(self, database: str):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def _create_client(self, credentials: ByteHouseCredentials):
        pass

    @abstractmethod
    def _set_client_database(self):
        pass

    @abstractmethod
    def _server_version(self):
        pass

    def _ensure_database(self, database_engine) -> None:
        if not self.database:
            return
        # TODO: log & exception handling
        self.command(f'CREATE DATABASE IF NOT EXISTS {self.database}')
        self._set_client_database()

    def _check_atomic_exchange(self) -> bool:
        return False
