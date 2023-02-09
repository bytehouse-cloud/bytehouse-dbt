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
from typing import Optional

import dbt.exceptions
from dbt.adapters.base.relation import BaseRelation, Policy


@dataclass
class ByteHouseQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass
class ByteHouseIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class ByteHouseRelation(BaseRelation):
    quote_policy: ByteHouseQuotePolicy = ByteHouseQuotePolicy()
    include_policy: ByteHouseIncludePolicy = ByteHouseIncludePolicy()
    quote_character: str = ''
    can_exchange: bool = False

    def __post_init__(self):
        if self.database != self.schema and self.database:
            raise dbt.exceptions.RuntimeException(
                f'Cannot set database {self.database} in bytehouse!'
            )

    def render(self):
        if self.include_policy.database and self.include_policy.schema:
            raise dbt.exceptions.RuntimeException(
                'Got a bytehouse relation with schema and database set to '
                'include, but only one can be set'
            )
        return super().render()

    def matches(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        identifier: Optional[str] = None,
    ):
        if schema:
            raise dbt.exceptions.RuntimeException(
                f'Passed unexpected schema value {schema} to Relation.matches'
            )
        return self.database == database and self.identifier == identifier
