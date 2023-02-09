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

{% macro bytehouse__get_catalog(information_schema, schemas) -%}
  {%- call statement('catalog', fetch_result=True) -%}
    select
      null as table_database,
      columns.database as table_schema,
      columns.table as table_name,
      if(tables.engine not in ('MaterializedView', 'View'), 'table', 'view') as table_type,
      tables.comment as table_comment,
      columns.name as column_name,
      columns.position as column_index,
      columns.type as column_type,
      columns.comment as column_comment,
      null as table_owner
    from system_meta.columns as columns
    join system_meta.tables as tables on tables.database = columns.database and tables.name = columns.table
    where database != 'system_meta' and
    (
    {%- for schema in schemas -%}
      columns.database = '{{ schema }}'
      {%- if not loop.last %} or {% endif -%}
    {%- endfor -%}
    )
    order by columns.database, columns.table, columns.position
  {%- endcall -%}
  {{ return(load_result('catalog').table) }}
{%- endmacro %}
