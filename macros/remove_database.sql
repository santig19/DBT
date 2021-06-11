{% macro remove_database(database_name) -%}
    {% set sql %}
        USE ROLE ACCOUNTADMIN;
        DROP DATABASE {{database_name}};
    {% endset %}
        
    {% do run_query(sql) %}
{%- endmacro %}