{% macro rename_database(database_name, new_database_name) -%}
    {% set sql %}
        USE ROLE ACCOUNTADMIN;
        ALTER DATABASE {{database_name}} RENAME TO {{new_database_name}};        
    {% endset %}
        
    {% do run_query(sql) %}
{%- endmacro %}