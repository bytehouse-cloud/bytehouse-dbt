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

{% macro bytehouse__any_value(expression) -%}
    any({{ expression }})
{%- endmacro %}


{% macro bytehouse__bool_or(expression) -%}
    any({{ expression }}) > 0
{%- endmacro %}


{% macro bytehouse__cast_bool_to_text(field) %}
    multiIf({{ field }} > 0, 'true', {{ field }} = 0, 'false', NULL)
{% endmacro %}


{% macro bytehouse__hash(field) -%}
    lower(hex(MD5(toString({{ field }} ))))
{%- endmacro %}


{%- macro bytehouse__last_day(date, datepart) -%}
    {{ dbt.dateadd('day', '-1', dbt.dateadd(datepart, '1', dbt.date_trunc(datepart, date)))}}
{%- endmacro -%}


{% macro bytehouse__split_part(string_text, delimiter_text, part_number) %}
    splitByChar('{{delimiter_text}}', {{ string_text }})[{{ part_number }}]
{% endmacro %}


{% macro bytehouse__replace(field, old_chars, new_chars) %}
   replaceAll({{ field }},'{{ old_chars }}','{{ new_chars }}')
{% endmacro %}


{% macro bytehouse__listagg(measure, delimiter_text, order_by_clause, limit_num) %}
  {{ exceptions.raise_compiler_error(
    'bytehouse does not support the listagg function.  See the groupArray function instead')
    }}
{% endmacro %}


{% macro bytehouse__array_construct(inputs, data_type) -%}
    {% if inputs|length > 0 %}
    [ {{ inputs|join(' , ') }} ]
    {% else %}
    emptyArray{{data_type}}()
    {% endif %}
{%- endmacro %}


{% macro bytehouse__array_append(array, new_element) -%}
    arrayPushBack({{ array }}, {{ new_element }})
{% endmacro %}


{% macro bytehouse__array_concat(array_1, array_2) -%}
   arrayConcat({{ array_1 }}, {{ array_2 }})
{% endmacro %}
