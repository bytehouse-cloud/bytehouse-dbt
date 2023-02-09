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

{% macro bytehouse__get_show_grant_sql(relation) %}
    SELECT access_type as privilege_type, user_name as grantee FROM system.grants WHERE table = '{{ relation.name }}'
    AND database = '{{ relation.schema }}'
{%- endmacro %}

{% macro bytehouse__call_dcl_statements(dcl_statement_list) %}
    {% for dcl_statement in dcl_statement_list %}
      {% call statement('dcl') %}
        {{ dcl_statement }};
      {% endcall %}
    {% endfor %}
{% endmacro %}

