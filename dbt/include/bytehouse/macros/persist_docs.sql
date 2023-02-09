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

{% macro one_alter_column_comment(relation, column_name, comment) %}
  alter table {{ relation }} comment column {{ column_name }} '{{ comment }}'
{% endmacro %}

{% macro bytehouse__alter_relation_comment(relation, comment) %}
  alter table {{ relation }} comment '{{ comment }}'
{% endmacro %}

{% macro bytehouse__persist_docs(relation, model, for_relation, for_columns) %}
  {%- if for_relation and config.persist_relation_docs() and model.description -%}
    {% do run_query(alter_relation_comment(relation, model.description)) %}
  {%- endif -%}

  {%- if for_columns and config.persist_column_docs() and model.columns -%}
    {%- for column_name in model.columns -%}
      {%- set comment = model.columns[column_name]['description'] -%}
      {% do run_query(one_alter_column_comment(relation, column_name, comment)) %}
    {%- endfor -%}
  {%- endif -%}
{% endmacro %}