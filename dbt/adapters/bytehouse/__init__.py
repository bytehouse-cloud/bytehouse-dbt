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

from dbt.adapters.base import AdapterPlugin

from dbt.adapters.bytehouse.column import ByteHouseColumn  # noqa
from dbt.adapters.bytehouse.connections import ByteHouseConnectionManager  # noqa
from dbt.adapters.bytehouse.credentials import ByteHouseCredentials
from dbt.adapters.bytehouse.impl import ByteHouseAdapter
from dbt.adapters.bytehouse.relation import ByteHouseRelation  # noqa
from dbt.include import bytehouse  # noqa

Plugin = AdapterPlugin(
    adapter=ByteHouseAdapter,
    credentials=ByteHouseCredentials,
    include_path=bytehouse.PACKAGE_PATH,
)
