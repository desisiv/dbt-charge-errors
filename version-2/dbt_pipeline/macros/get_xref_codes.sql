{% macro get_xref_codes(code_type_value, return_field) -%}
    {% set xref_codes_query %}
    select distinct
        xref.{{ return_field }}
    from {{ var('analytics_catalog') }}.{{ var('analytics_schema')}}.cewlxref xref
    where xref.lookup_field_type = '{{ code_type_value }}'
      and xref.lookup_field is not null
    {% endset %}

    {% set xref_code_results = run_query(xref_codes_query) %}

    {% if execute %}
    {# Return the first column #}
    {% set xref_code_results_list = xref_code_results.columns[0].values() %}
    {% else %}
    {% set xref_code_results_list = [] %}
    {% endif %}

    {{ return(xref_code_results_list)}}
{%- endmacro %}