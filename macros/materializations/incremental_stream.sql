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

{#-- Get parameter values --#}
{%- set src_list = config.get('src_list') -%}
{%- set unique_key = config.get('unique_key') -%}
{%- set streams = [] -%}
{#-- Drop Target Table in full-refresh mode  --#}
{% if full_refresh_mode -%} 
    DROP TABLE IF EXISTS {{ this }};
{% endif %}

{% for src in src_list %}
    {#-- Get the source & target table name --#}
    {%- set src_table = '' -%}
    {%- if src[0] == 'source' -%} 
        {%- set source_table = source(src[1], src[2]) -%} 
    {%- else -%} 
        {%- set source_table = ref(src[1]) -%} 
    {%- endif -%}
    {%- set target_table = this.database + '.' + this.schema + '.' + this.name -%}
    {%- set target_stream = this.database + '.' + this.schema + '.'+ incr_stream.get_stream_name(this.table, source_table.name) -%}
    {%- set _ = streams.append(target_stream) %}
    {#-- Drop Stream in full-refresh mode  --#}
    {% if full_refresh_mode -%} 
        DROP STREAM IF EXISTS {{target_stream}};
    {% endif %}
    {#-- CREATE OBJECTS (STREAM, TABLE) IF NOT EXISTS  --#}
    CREATE STREAM IF NOT EXISTS {{target_stream}} ON TABLE {{source_table}};
{% endfor %}

{%- set target_view = this.database + '.' + this.schema + '.V_' + this.name -%}

{%- set is_data_in_streams = 0 -%}
{% set relation_exists = load_relation(this) is not none %}
{%- if full_refresh_mode or not relation_exists%} 
CREATE OR REPLACE VIEW {{target_view}}
    AS ({{ sql }});
CREATE TABLE IF NOT EXISTS {{ this }} AS SELECT * FROM {{target_view}};
{%- else -%}
{#-- Check the presence of records to merge --#}
{%- set is_data_in_streams -%}
    select ({% for stream in streams %} 
    system$stream_has_data('{{stream}}'){%- if not loop.last %} OR {% endif %}
    {% endfor %})
{%- endset -%}
{%- set results = run_query(is_data_in_streams) -%}

{%- if execute -%}
        {% if results|length > 0 %}
        {%- set is_data_in_streams = results.columns[0].values()[0] -%}
        {%- else -%}
        {{ log("  => Expected streams not found " , info=True) }}
        {%- set is_data_in_streams = False -%}
        {%- endif -%}
{%- else -%}
        {%- set is_data_in_streams = 0 -%}
{%- endif -%}


{%- endif -%}

{#-- Execute Merge only if new records in stream  --#}
{%- if is_data_in_streams  -%}
{{ log("  => record(s) to merge" , info=True) }}
CREATE OR REPLACE VIEW {{target_view}}
    AS ({{ sql }});

    {%- set columns = adapter.get_columns_in_relation(target_view) | rejectattr('name', 'equalto', 'METADATA$ACTION')
                | rejectattr('name', 'equalto', 'METADATA$ISUPDATE')
                | rejectattr('name', 'equalto', 'METADATA$ROW_ID')
                | list %}
    {%- if unique_key -%}
    MERGE INTO  {{ this }} TARGET
    USING {{target_view}} SOURCE
    ON ({% for key in unique_key %}
    TARGET.{{ key }} = SOURCE.{{ key }}{%- if not loop.last %} AND {% endif %}{% endfor %}
    )
    -- Mode DELETE
    WHEN MATCHED AND SOURCE.METADATA$ACTION = 'DELETE' AND SOURCE.METADATA$ISUPDATE = 'FALSE' THEN
        DELETE
    -- Mode UPDATE
    WHEN MATCHED AND SOURCE.METADATA$ACTION = 'INSERT' THEN 
    UPDATE SET {%- for col in columns %}
    TARGET.{{col.name}} = SOURCE.{{col.name}}{%- if not loop.last %},{% endif %}{% endfor %}
    -- Mode INSERT
    WHEN NOT MATCHED AND SOURCE.METADATA$ACTION = 'INSERT' THEN INSERT
    ( {% for col in columns%}
    {{col.name}}{%- if not loop.last %},{% endif %}{% endfor %}
    )
    VALUES
    ({% for col in columns%}
    SOURCE.{{col.name}}{%- if not loop.last %},{% endif %}{% endfor %}
    )
    ;
    {% else %}
        INSERT INTO {{ this }}  
        SELECT {% for col in columns%}
        SOURCE.{{col.name}}{%- if not loop.last %},{% endif %}{% endfor %} FROM {{target_view}} SOURCE WHERE SOURCE.METADATA$ACTION = 'INSERT' 
        AND SOURCE.METADATA$ISUPDATE = 'FALSE';
    {%- endif -%}

    
{%- else -%}
    {{ log("  => No record(s) to merge input streams are empty " , info=True) }}
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
