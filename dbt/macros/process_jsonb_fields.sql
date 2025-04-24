{% macro process_jsonb_fields(source_table, content_column='"CONTENT"') %}

{# Get all keys from the JSONB content column #}
{% set keys_query %}
  SELECT DISTINCT jsonb_object_keys({{ content_column }}) AS key
  FROM {{ source_table }}
{% endset %}

{% set keys_results = run_query(keys_query) %}
{% set keys = [] %}
{% if keys_results and keys_results.rows %}
  {% for row in keys_results.rows %}
    {% do keys.append(row[0]) %}
  {% endfor %}
{% endif %}

{# Generate the SQL query #}
SELECT
  '{{ key }}' as id
  {% for key in keys %}
    {% if key|lower != 'id' %}
    ,{{ content_column }} ->> '{{ key }}' AS "{{ key }}"
    {% endif %}
  {% endfor %}
FROM {{ source_table }}

{% endmacro %} 