/*
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
*/

{% macro bytehouse__test_relationships(model, column_name, to, field) %}

with child as (
    select {{ column_name }} as from_field
    from {{ model }}
    where {{ column_name }} is not null
),

parent as (
    select {{ field }} as to_field
    from {{ to }}
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null
settings join_use_nulls = 1

{% endmacro %}
