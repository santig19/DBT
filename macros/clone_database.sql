{% macro clone_database(database_name) -%}
    {% set sql %}
        USE ROLE ACCOUNTADMIN;
        CREATE OR REPLACE DATABASE test CLONE {{database_name}};
    {% endset %}
        
    {% do run_query(sql) %}
{%- endmacro %}