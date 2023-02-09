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

{% macro bytehouse__load_csv_rows(model, agate_table) %}
  {% set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) %}
  {% set data_sql = adapter.get_csv_data(agate_table) %}

  {% set sql -%}
    ByteHouseCustomCSV|insert into {{ this.render() }} ({{ cols_sql }}){{ adapter.get_model_settings(model) }} VALUES{{ data_sql }}
  {%- endset %}

  {% do adapter.add_query(sql, bindings=agate_table, abridge_sql_log=True) %}
{% endmacro %}

{% macro bytehouse__create_csv_table(model, agate_table) %}
  {%- set column_override = model['config'].get('column_types', {}) -%}
  {%- set quote_seed_column = model['config'].get('quote_columns', None) -%}

  {% set sql %}
    {% call statement('main') %}
       drop table if exists {{ this.render() }}
    {% endcall %}
    create table {{ this.render() }} (
      {%- for col_name in agate_table.column_names -%}
        {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
        {%- set type = column_override.get(col_name, inferred_type) -%}
        {%- set column_name = (col_name | string) -%}
          {{ adapter.quote_seed_column(column_name, quote_seed_column) }} {{ type }} {%- if not loop.last -%}, {%- endif -%}
      {%- endfor -%}
    )
    {{ engine_clause(label='engine') }}
    {{ order_cols(label='order by') }}
    {{ partition_cols(label='partition by') }}
  {% endset %}

  {% call statement('_') -%}
    {{ sql }}
  {%- endcall %}

  {{ return(sql) }}
{% endmacro %}
