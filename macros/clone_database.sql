{% macro clone_database(database_name, database_clone_name) -%}
    {% set sql %}
        USE ROLE ACCOUNTADMIN;
        CREATE OR REPLACE DATABASE {{database_clone_name}} CLONE {{database_name}};
    {% endset %}
        
    {% do run_query(sql) %}
{%- endmacro %}