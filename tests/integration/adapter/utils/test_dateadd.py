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
from dbt.tests.adapter.utils.test_dateadd import BaseDateAdd

# We remove the null row from this test because (1) nullables work fine with Nullable ClickHouse types, but
# (2) dealing with all the type conversions is ugly
seeds__data_dateadd_csv = """from_time,interval_length,datepart,result
2018-01-01 01:00:00,1,day,2018-01-02 01:00:00
2018-01-01 01:00:00,1,month,2018-02-01 01:00:00
2018-01-01 01:00:00,1,year,2019-01-01 01:00:00
2018-01-01 01:00:00,1,hour,2018-01-01 02:00:00
"""


class TestDateAdd(BaseDateAdd):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_dateadd.csv": seeds__data_dateadd_csv}
