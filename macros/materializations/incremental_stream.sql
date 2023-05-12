{#-- ###################################################### --#}
{#-- Merge of stream to the target                          --#}
{#-- Main parameters :                                      --#}
{#--    1) sql = SELECT statement with source or ref tables --#}
{#--    2) src_table = source or ref table  		        --#}
{#--    3) unique_key = PK used for the merge               --#}
{#--    3) sql = SQL used for the transformation            --#}
{#-- ###################################################### --#}

{%- macro merge_stream_sql(target_relation, src, unique_key, sql) -%}
{%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}
{% if full_refresh_mode -%} 
DROP TABLE IF EXISTS {{ this }};
{% endif %}

{#-- Get parameter values --#}
{%- set streams = config.get('streams') -%}
{{ log("streams" , info=True) }}
{%- set v_count = 1 -%}
{% for source, src_table in streams  %}
{{ log("source" ~src_table , info=True) }}
{%- if source -%} 
    {%- set source_table = source(source,src_table) -%}
{%- else -%} 
    {%- set source_table = ref(src_table) -%} 
{%- endif -%}
    {%- set target_stream = this.database + '.' + this.schema + '.'+ incr_stream.get_stream_name(this.table, src_table) -%}
    {% if full_refresh_mode %} 
DROP STREAM IF EXISTS {{target_stream}};
    {% endif  %}
CREATE STREAM IF NOT EXISTS {{target_stream}} ON TABLE {{source_table}} APPEND_ONLY=TRUE;
{% endfor %}


{%- set unique_key = config.get('unique_key') -%}

{%- set target_table = this.database + '.' + this.schema + '.' + this.name -%}
{%- set target_view = this.database + '.' + this.schema + '.V_' + this.name -%}

{#-- CREATE OBJECTS (STREAM, TABLE) IF NOT EXISTS  --#}

CREATE TABLE IF NOT EXISTS {{ this }} AS SELECT * FROM {{target_view}};


{%- if full_refresh_mode %} 
CREATE OR REPLACE VIEW {{target_view}}
    AS ({{ sql }});
INSERT INTO {{this}} SELECT * FROM {{target_view}};
{%- else -%}



{{ log("  ===> " +v_count | string + " record(s) to merge" , info=True) }}
{%- endif -%}

{#-- Execute Merge only if new records in stream  --#}
{%- if v_count > 0  -%}
CREATE OR REPLACE VIEW {{target_view}}
    AS ({{ sql }});
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
    {{ log("  => No new data available in streams" , info=True) }}
{%- endif -%}
{%- endmacro %}




{%- materialization incremental_stream, adapter='snowflake' -%}
  {%- set src = config.get('src') -%}
  {%- set src_table = config.get('src_table') -%}

  {%- set unique_key = config.get('unique_key') -%}

  {%- set target_relation = this -%}
  {%- set existing_relation = load_relation(this) -%}

  -- setup
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {#-- Create the MERGE --#}
  {%- set build_sql = incr_stream.merge_stream_sql(target_relation, src, unique_key, sql) -%}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
