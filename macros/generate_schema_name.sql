
{% macro generate_schema_name(custom_schema_name, node) -%}
    {# Si tu définis schema au niveau du modèle/dossier, on l'utilise tel quel, sinon on prend target.schema #}
    {{ return(custom_schema_name if custom_schema_name else target.schema) }}
{%- endmacro %}
