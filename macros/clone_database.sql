{% macro clone_database(database_name) -%}    
    USE ROLE ACCOUNTADMIN; 
    CREATE OR REPLACE DATABASE test CLONE {{database_name}};    
{%- endmacro %}