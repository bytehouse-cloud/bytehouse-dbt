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

{% macro bytehouse__snapshot_hash_arguments(args) -%}
  halfMD5({%- for arg in args -%}
    coalesce(cast({{ arg }} as varchar ), '')
    {% if not loop.last %} || '|' || {% endif %}
  {%- endfor -%})
{%- endmacro %}

{% macro bytehouse__snapshot_string_as_time(timestamp) -%}
  {%- set result = "toDateTime('" ~ timestamp ~ "')" -%}
  {{ return(result) }}
{%- endmacro %}


{% macro bytehouse__post_snapshot(staging_relation) %}
    {{ drop_relation_if_exists(staging_relation) }}
{% endmacro %}

{% macro build_snapshot_staging_table(strategy, sql, target_relation) %}
    {% set tmp_relation = make_temp_relation(target_relation) %}

    {% set select = snapshot_staging_table(strategy, sql, target_relation) %}

    {% call statement('build_snapshot_staging_relation') %}
        {{ create_table_as(False, tmp_relation, select) }}
    {% endcall %}

    {% do return(tmp_relation) %}
{% endmacro %}

{% macro bytehouse__snapshot_merge_sql(target, source, insert_cols) -%}
  {%- set insert_cols_csv = insert_cols | join(', ') -%}
  {%- set valid_to_col = adapter.quote('dbt_valid_to') -%}

  {%- set upsert = target ~ '__snapshot_upsert' -%}
  {% call statement('main') %}
     drop table if exists {{ upsert }}
  {% endcall %}
  {% call statement('create_upsert_relation') %}
    create table {{ upsert }} as {{ target }}
  {% endcall %}

  {% call statement('insert_unchanged_data') %}
    insert into {{ upsert }} ({{ insert_cols_csv }})
    select {% for column in insert_cols -%}
      {{ column }} {%- if not loop.last %}, {%- endif %}
    {%- endfor %}
    from {{ target }}
    where dbt_scd_id not in (
      select {{ source }}.dbt_scd_id from {{ source }} 
    )
  {% endcall %}

 {% call statement('insert_updated_and_deleted') %}
    insert into {{ upsert }} ({{ insert_cols_csv }})
    with updates_and_deletes as (
      select
        dbt_scd_id,
        dbt_valid_to
      from {{ source }}
      where dbt_change_type IN ('update', 'delete')
    )
    select {% for column in insert_cols %}
      {%- if column == valid_to_col -%}
        updates_and_deletes.dbt_valid_to as dbt_valid_to
      {%- else -%}
        target.{{ column }} as {{ column }}
      {%- endif %} {%- if not loop.last %}, {%- endif %}
    {%- endfor %}
    from {{ target }} target
    join updates_and_deletes on target.dbt_scd_id = updates_and_deletes.dbt_scd_id;
  {% endcall %}

  {% call statement('insert_new') %}
    insert into {{ upsert }} ({{ insert_cols_csv }})
    select {% for column in insert_cols -%}
      {{ column }} {%- if not loop.last %}, {%- endif %}
    {%- endfor %}
    from {{ source }}
    where {{ source }}.dbt_change_type IN ('insert');
  {% endcall %}

  {% if target.can_exchange %}
    {% do exchange_tables_atomic(upsert, target) %}
    {% call statement('drop_exchanged_relation') %}
      drop table if exists {{ upsert }};
    {% endcall %}
  {% else %}
    {% call statement('drop_target_relation') %}
      drop table if exists {{ target }};
    {% endcall %}
    {% call statement('rename_upsert_relation') %}
      rename table {{ upsert }} to {{ target }};
    {% endcall %}
  {% endif %}

  {% do return ('select 1') %}
{% endmacro %}
