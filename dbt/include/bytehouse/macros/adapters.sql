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

{% macro bytehouse__create_view_as(relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create view {{ relation.include(database=False) }} {{ on_cluster_clause(label="on cluster") }}
  as (
    {{ sql }}
  )
{%- endmacro %}

{% macro bytehouse__list_schemas(database) %}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
    select name from system_meta.databases
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro bytehouse__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    create database if not exists {{ relation.without_identifier().include(database=False) }}
        {{ on_cluster_clause(label="on cluster") }}
        {{ adapter.bytehouse_db_engine_clause() }}
  {% endcall %}
{% endmacro %}

{% macro bytehouse__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}
    drop database if exists {{ relation.without_identifier().include(database=False) }} {{ on_cluster_clause(label="on cluster") }}
  {%- endcall -%}
{% endmacro %}

{% macro bytehouse__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      t.name as name,
      t.database as schema,
      if(t.type not in ('MaterializedView', 'View', 'VIEW'), 'table', 'view') as type,
      db.engine as db_engine
    from system_meta.tables as t JOIN system_meta.databases as db on t.database = db.name
    where schema = '{{ schema_relation.schema }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro bytehouse__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
    select
      name,
      type,
      position
    from system_meta.columns
    where
      table = '{{ relation.identifier }}'
    {% if relation.schema %}
      and database = '{{ relation.schema }}'
    {% endif %}
    order by position
  {% endcall %}
  {% do return(load_result('get_columns_in_relation').table) %}
{% endmacro %}

{% macro bytehouse__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop table if exists {{ relation }} {{ on_cluster_clause(label="on cluster") }}
  {%- endcall %}
{% endmacro %}

{% macro bytehouse__rename_relation(from_relation, to_relation) -%}
  {% call statement('drop_relation') %}
    drop table if exists {{ to_relation }} {{ on_cluster_clause(label="on cluster") }}
  {% endcall %}
  {% call statement('rename_relation') %}
    rename table {{ from_relation }} to {{ to_relation }} {{ on_cluster_clause(label="on cluster") }}
  {% endcall %}
{% endmacro %}

{% macro bytehouse__truncate_relation(relation) -%}
  {% call statement('truncate_relation') -%}
    truncate table {{ relation }}
  {%- endcall %}
{% endmacro %}

{% macro bytehouse__make_temp_relation(base_relation, suffix) %}
  {% set tmp_identifier = base_relation.identifier ~ suffix %}
  {% set tmp_relation = base_relation.incorporate(
                              path={"identifier": tmp_identifier, "schema": None}) -%}
  {% do return(tmp_relation) %}
{% endmacro %}


{% macro bytehouse__generate_database_name(custom_database_name=none, node=none) -%}
  {% do return(None) %}
{%- endmacro %}

{% macro bytehouse__current_timestamp() -%}
  now()
{%- endmacro %}

{% macro bytehouse__get_columns_in_query(select_sql) %}
  {% call statement('get_columns_in_query', fetch_result=True, auto_begin=False) -%}
    select * from (
        {{ select_sql }}
    ) as __dbt_sbq
    limit 0
  {% endcall %}

  {{ return(load_result('get_columns_in_query').table.columns | map(attribute='name') | list) }}
{% endmacro %}

{% macro bytehouse__alter_column_type(relation, column_name, new_column_type) -%}
  {% call statement('alter_column_type') %}
    alter table {{ relation }} {{ on_cluster_clause(label="on cluster") }} modify column {{ adapter.quote(column_name) }} {{ new_column_type }}
  {% endcall %}
{% endmacro %}

{% macro exchange_tables_atomic(old_relation, target_relation) %}
  {%- call statement('exchange_tables_atomic') -%}
    EXCHANGE TABLES {{ old_relation }} AND {{ target_relation }}
  {% endcall %}
{% endmacro %}

