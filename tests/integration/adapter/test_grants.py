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

from dbt.tests.adapter.grants.test_incremental_grants import BaseIncrementalGrants
from dbt.tests.adapter.grants.test_invalid_grants import BaseInvalidGrants
from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants
from dbt.tests.adapter.grants.test_snapshot_grants import BaseSnapshotGrants


class TestModelGrants(BaseModelGrants):
    pass


class TestIncrementalGrants(BaseIncrementalGrants):
    pass


class TestSeedGrants(BaseSeedGrants):
    pass


class TestInvalidGrants(BaseInvalidGrants):
    def grantee_does_not_exist_error(self):
        return "511"

    # ClickHouse doesn't give a very specific error for an invalid privilege
    def privilege_does_not_exist_error(self):
        return "Syntax error"


class TestSnapshotGrants(BaseSnapshotGrants):
    pass
