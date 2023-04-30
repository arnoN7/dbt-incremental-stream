{#-- ###################################################### --#}
{#-- Merge of stream to the target                          --#}
{#-- Main parameters :                                      --#}
{#--    1) sql = SELECT statement with source or ref tables --#}
{#--    2) src_table = source or ref table  		        --#}
{#--    3) unique_key = PK used for the merge               --#}
{#-- ###################################################### --#}

{%- macro merge_stream_sql(target_relation, src, unique_key, sql) -%}
{%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}

{#-- Get parameter values --#}
{%- set src = config.get('src') -%}
{%- set src_table = config.get('src_table') -%}
{%- set unique_key = config.get('unique_key') -%}

{#-- Get the source & target table name --#}
{%- if src -%} 
    {%- set source_table = source(src,src_table) -%} 
{%- else -%} 
    {%- set source_table = ref(src_table) -%} 
{%- endif -%}

{%- set target_table = this.database + '.' + this.schema + '.' + this.name -%}
{%- set target_stream = this.database + '.' + this.schema + '.S_' + this.name -%}
{%- set target_view = this.database + '.' + this.schema + '.V_' + this.name -%}
{#-- CREATE OBJECTS (STREAM, TABLE) IF NOT EXISTS  --#}
{% if full_refresh_mode -%} 
DROP TABLE {{ this }};
DROP STREAM {{target_stream}};
{% endif %}
CREATE STREAM IF NOT EXISTS {{target_stream}} ON TABLE {{source_table}} APPEND_ONLY=TRUE;
CREATE OR REPLACE VIEW {{target_view}}
    AS ({{ sql }});

CREATE TABLE IF NOT EXISTS {{ this }} AS SELECT * FROM {{target_view}};

{%- set v_count = 0 -%}
{%- if full_refresh_mode %} 
INSERT INTO {{this}} SELECT * FROM {{target_view}};
{%- else -%}
{#-- Check the presence of records to merge --#}
{%- set v_count -%}
        select count(1) from {{ target_stream }}
{%- endset -%}
{%- set results = run_query(v_count) -%}

{%- if execute -%}
        {# Return the first column #}
        {%- set v_count = results.columns[0].values()[0] -%}
{%- else -%}
        {%- set v_count = 0 -%}
{%- endif -%}


{#-- In case of mode mode RUN --#}
{{ log("  => " +v_count | string + " record(s) to merge" , info=True) }}
{%- endif -%}

{%- if v_count > 0  -%}
    {%- set s_count = v_count | string() -%}

    {%- set columns = adapter.get_columns_in_relation(target_view) %}
    MERGE INTO  {{ this }} TARGET
    USING {{target_view}} SOURCE
    ON ({% for key in unique_key %}
    TARGET.{{ key }} = SOURCE.{{ key }}{%- if not loop.last %} AND {% endif %}{% endfor %}
    )
    -- Mode UPDATE
    WHEN MATCHED THEN UPDATE
    SET {%- for col in columns %}
    TARGET.{{col.name}} = SOURCE.{{col.name}}{%- if not loop.last %},{% endif %}{% endfor %}
    -- Mode INSERT
    WHEN NOT MATCHED THEN INSERT
    ( {% for col in columns%}
    {{col.name}}{%- if not loop.last %},{% endif %}{% endfor %}
    )
    VALUES
    ({% for col in columns%}
    SOURCE.{{col.name}}{%- if not loop.last %},{% endif %}{% endfor %}
    )
    ;
    
{%- else -%}
    {{ log("  => No new data available in " + target_stream , info=True) }}
{%- endif -%}
{%- endmacro %}




{%- materialization incremental_stream, adapter='snowflake' -%}
  {%- set src = config.get('src') -%}
  {%- set src_table = config.get('src_table') -%}
  {%- if src -%} 
        {%- set source_table = source(src,src_table) -%} 
  {%- else -%} 
        {%- set source_table = ref(src_table) -%} 
  {%- endif -%}

  {%- set unique_key = config.get('unique_key') -%}

  {%- set target_relation = this -%}
  {%- set existing_relation = load_relation(this) -%}

  -- setup
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {#-- Create the MERGE --#}
  {%- set build_sql = merge_stream_sql(target_relation, src, unique_key, sql) -%}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
