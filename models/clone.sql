{{
  config(
    pre_hook=clone_database(var('production_database_name'))    
  )
}}