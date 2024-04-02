{% macro crd_indicator(errtext_field, transstsmne_field) -%}
    {% set err_text_list = ["No Charge with provided  External Charge Interface Number exists.", "No Charge with provided  External Charge Interface Number exists."] %}

    case
        {% for err_text in err_text_list %}
        when {{ errtext_field }} = '{{ err_text }}' and {{ transstsmne_field }} = 'Err' then 'Y'
        {% endfor %}
        else 'N'
    end
{%- endmacro %}
