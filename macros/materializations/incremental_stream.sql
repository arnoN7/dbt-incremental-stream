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
{%- set target_stream = this.database + '.' + this.schema + '.'+ incr_stream.get_stream_name(this.table, src_table) -%}
{%- set target_view = this.database + '.' + this.schema + '.V_' + this.name -%}
{#-- Drop Target Table and Stream in full-refresh mode  --#}
{% if full_refresh_mode -%} 
DROP TABLE IF EXISTS {{ this }};
DROP STREAM IF EXISTS {{target_stream}};
{% endif %}
{#-- CREATE OBJECTS (STREAM, TABLE) IF NOT EXISTS  --#}
CREATE STREAM IF NOT EXISTS {{target_stream}} ON TABLE {{source_table}};

{%- set v_count = 0 -%}
{% set relation_exists = load_relation(this) is not none %}
{%- if full_refresh_mode or not relation_exists%} 
CREATE OR REPLACE VIEW {{target_view}}
    AS ({{ sql }});
CREATE TABLE IF NOT EXISTS {{ this }} AS SELECT * FROM {{target_view}};
{%- else -%}
{#-- Check the presence of records to merge --#}
{%- set v_count -%}
        select count(1) from {{ target_stream }}
{%- endset -%}
{%- set results = run_query(v_count) -%}

{%- if execute -%}
        {%- set v_count = results.columns[0].values()[0] -%}
{%- else -%}
        {%- set v_count = 0 -%}
{%- endif -%}

{{ log("  => " +v_count | string + " record(s) to merge" , info=True) }}
{%- endif -%}

{#-- Execute Merge only if new records in stream  --#}
{%- if v_count > 0  -%}
CREATE OR REPLACE VIEW {{target_view}}
    AS ({{ sql }});
    {%- set s_count = v_count | string() -%}

    {%- set columns = adapter.get_columns_in_relation(target_view) | rejectattr('name', 'equalto', 'METADATA$ACTION')
                | rejectattr('name', 'equalto', 'METADATA$ISUPDATE')
                | rejectattr('name', 'equalto', 'METADATA$ROW_ID')
                | list %}
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
    
{%- else -%}
    {{ log("  => No new data available in " + target_stream , info=True) }}
{%- endif -%}
{%- endmacro %}

{% macro sfc_get_stream_merge_sql(target_relation, source_relation, unique_key) -%}
    {#-- Don't include the Snowflake Stream metadata columns --#}
    {% set dest_columns = adapter.get_columns_in_relation(target_relation)
                | rejectattr('name', 'equalto', 'METADATA$ACTION')
                | rejectattr('name', 'equalto', 'METADATA$ISUPDATE')
                | rejectattr('name', 'equalto', 'METADATA$ROW_ID')
                | list %}
    {% set dest_cols_csv =  get_quoted_csv(dest_columns | map(attribute="name")) -%}

    MERGE INTO {{ target_relation }} T
    USING {{ source_relation }} S

    {% if unique_key -%}
        ON (T.{{ unique_key }} = S.{{ unique_key }})
    {% else -%}
        ON FALSE
    {% endif -%}

    {% if unique_key -%}
    WHEN MATCHED AND S.METADATA$ACTION = 'DELETE' AND S.METADATA$ISUPDATE = 'FALSE' THEN
        DELETE
    WHEN MATCHED AND S.METADATA$ACTION = 'INSERT' AND S.METADATA$ISUPDATE = 'TRUE' THEN
        UPDATE SET
        {% for column in dest_columns -%}
            T.{{ adapter.quote(column.name) }} = S.{{ adapter.quote(column.name) }}
            {% if not loop.last -%}, {% endif -%}
        {% endfor -%}
    {% endif -%}

    WHEN NOT MATCHED AND S.METADATA$ACTION = 'INSERT' AND S.METADATA$ISUPDATE = 'FALSE' THEN
        INSERT
        ({{ dest_cols_csv }})
        VALUES
        ({{ dest_cols_csv }})

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
