{% macro serv_prov_flag(proc_code_field, sp_metadata1_field) -%}
    {% set lab_codes = get_xref_codes('Lab Codes', 'lookup_field') %}
    {% set lab_exception_codes = get_xref_codes('Lab Exception Codes', 'lookup_field') %}

    case
        {% for lab_code in lab_codes %}
        when {{ proc_code_field }} = '{{ lab_code }}' then 'Lab'
        {% endfor %}
        {% for lab_exception_code in lab_exception_codes %}
        when {{ proc_code_field }} = '{{ lab_exception_code }}' then 'Amb'
        {% endfor %}
        when {{ sp_metadata1_field }} is null then 'Review SP'
        else {{ sp_metadata1_field }}
    end
{%- endmacro %}