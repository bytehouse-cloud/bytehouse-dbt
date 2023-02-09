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

from dataclasses import dataclass
from typing import Any, Dict, Optional

from dbt.contracts.connection import Credentials

import dbt


@dataclass
class ByteHouseCredentials(Credentials):
    """
    ByteHouse connection credentials data class.
    """

    driver: Optional[str] = None
    host: str = 'localhost'
    port: Optional[int] = None
    region: Optional[str] = None
    account: Optional[str] = None
    user: Optional[str] = 'default'
    retries: int = 1
    database: Optional[str] = None
    schema: Optional[str] = 'default'
    password: str = ''
    warehouse: Optional[str] = None
    cluster: Optional[str] = None
    database_engine: Optional[str] = None
    cluster_mode: bool = False
    secure: bool = False
    verify: bool = True
    connect_timeout: int = 10
    send_receive_timeout: int = 300
    sync_request_timeout: int = 5
    compress_block_size: int = 1048576
    compression: str = ''
    check_exchange: bool = True
    custom_settings: Optional[Dict[str, Any]] = None

    @property
    def type(self):
        return 'bytehouse'

    @property
    def unique_field(self):
        return self.host

    def __post_init__(self):
        if self.database is not None and self.database != self.schema:
            raise dbt.exceptions.RuntimeException(
                f'    schema: {self.schema} \n'
                f'    database: {self.database} \n'
                f'    cluster: {self.cluster} \n'
                f'On ByteHouse, database must be omitted or have the same value as'
                f' schema.'
            )
        self.database = None

    def _connection_keys(self):
        return (
            'driver',
            'host',
            'port',
            'region',
            'account',
            'user',
            'schema',
            'retries',
            'database_engine',
            'cluster_mode',
            'secure',
            'verify',
            'connect_timeout',
            'send_receive_timeout',
            'sync_request_timeout',
            'compress_block_size',
            'compression',
            'check_exchange',
            'custom_settings',
        )
