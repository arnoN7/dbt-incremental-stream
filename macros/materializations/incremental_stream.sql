{#-- ###################################################### --#}
{#-- Merge of stream to the target                          --#}
{#-- Main parameters :                                      --#}
{#--    1) sql = SELECT statement with source or ref tables --#}
{#--    2) src_table = source or ref table  		        --#}
{#--    3) unique_key = PK used for the merge               --#}
{#--    3) sql = SQL used for the transformation            --#}
{#-- ###################################################### --#}

{%- macro merge_stream_sql(target_relation, sql, tmp_relation, unique_key) -%}
{%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}

{#-- Get parameter values --#}
{% set target_relation = this %}
{%- set src_list = config.get('src_list') -%}
{% set grant_config = config.get('grants') %}
{% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}
{%- set streams = [] -%}
{%- set materialization = 'table' -%}
{#-- Drop Target Table in full-refresh mode  --#}
{% if full_refresh_mode -%} 
    DROP TABLE IF EXISTS {{ target_relation }};
{% endif %}

{% for src in src_list %}
    {#-- Get the source & target table name --#}
    {%- set src_table = '' -%}
    {%- if src['mode'] == 'source' -%} 
        {%- set source_table = load_relation(source(src['source_name'], src['table_name'])) -%}
        {#-- to avoid quoted tables --#}
        {%- set source_table_raw = source(src['source_name'], src['table_name']) -%}
    {%- else -%}
        {%- set source_table = load_relation(ref(src['table_name'])) -%}
        {#-- to avoid quoted tables --#}
        {%- set source_table_raw = ref(src['table_name']) -%}
    {%- endif -%}
    {%- if source_table.is_view -%}
      {%- set materialization = 'view' -%}
    {%- endif -%}
    {%- set target_table = target_relation -%}
    {%- set target_stream = target_relation.include(identifier=false) | string + '.'+ incr_stream.get_stream_name(target_relation.include(database=false, schema=false) | string, source_table_raw.include(database=false, schema=false) | string) -%}
    {%- set _ = streams.append(target_stream) %}
    {#-- Drop Stream in full-refresh mode  --#}
    {% if full_refresh_mode -%}
        DROP STREAM IF EXISTS {{target_stream}};
    {% endif %}
    {#-- CREATE OBJECTS (STREAM, TABLE) IF NOT EXISTS  --#}
    {%- set stream_type = src['stream_type'] | upper -%}
    CREATE STREAM IF NOT EXISTS {{target_stream}} ON {{materialization}} {{source_table_raw}} {%- if var('TIMESTAMP', False) %} AT (TIMESTAMP => TO_TIMESTAMP_TZ('{{var('TIMESTAMP')}}', 'yyyy-mm-dd hh24:mi:ss')){%- endif -%}{%- if stream_type in ['APPEND_ONLY', 'INSERT_ONLY'] %} {{stream_type}} = TRUE{%- endif -%};
{% endfor %}



{%- set is_data_in_streams = 0 -%}
{% set relation_exists = load_relation(target_relation) is not none %}
{%- if full_refresh_mode or not relation_exists%}
CREATE TABLE IF NOT EXISTS {{ target_relation }} AS SELECT * FROM ({{sql}});
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
    {%- call statement('create_tmp_relation') -%}
        {{ snowflake__create_view_as_with_temp_flag(tmp_relation, sql, True) }}
    {%- endcall -%}

    {%- set columns = adapter.get_columns_in_relation(tmp_relation) | rejectattr('name', 'equalto', 'METADATA$ACTION')
                | rejectattr('name', 'equalto', 'METADATA$ISUPDATE')
                | rejectattr('name', 'equalto', 'METADATA$ROW_ID')
                | list %}
    {%- if unique_key -%}
    {#-- Create a lookup map of column names (uppercase) to their actual names --#}
    {%- set column_map = {} -%}
    {%- for col in columns -%}
        {%- set _ = column_map.update({col.name|upper: col.name}) -%}
    {%- endfor -%}
    MERGE INTO  {{ target_relation }} TARGET
    USING {{tmp_relation}} SOURCE
    ON ({% for key in unique_key %}
    {%- set actual_key = column_map.get(key|upper, key) -%}
    TARGET.{{ adapter.quote(actual_key) }} = SOURCE.{{ adapter.quote(actual_key) }}{%- if not loop.last %} AND {% endif %}{% endfor %}
    )
    -- Mode DELETE
    WHEN MATCHED AND SOURCE.METADATA$ACTION = 'DELETE' AND SOURCE.METADATA$ISUPDATE = 'FALSE' THEN
        DELETE
    -- Mode UPDATE
    WHEN MATCHED AND SOURCE.METADATA$ACTION = 'INSERT' THEN
    UPDATE SET {%- for col in columns %}
    TARGET.{{ adapter.quote(col.name) }} = SOURCE.{{ adapter.quote(col.name) }}{%- if not loop.last %},{% endif %}{% endfor %}
    -- Mode INSERT
    WHEN NOT MATCHED AND SOURCE.METADATA$ACTION = 'INSERT' THEN INSERT
    ( {% for col in columns%}
    {{ adapter.quote(col.name) }}{%- if not loop.last %},{% endif %}{% endfor %}
    )
    VALUES
    ({% for col in columns%}
    SOURCE.{{ adapter.quote(col.name) }}{%- if not loop.last %},{% endif %}{% endfor %}
    )
    ;
    {% else %}
        INSERT INTO {{ target_relation }}
        SELECT {% for col in columns%}
        SOURCE.{{ adapter.quote(col.name) }}{%- if not loop.last %},{% endif %}{% endfor %} FROM {{tmp_relation}} SOURCE WHERE SOURCE.METADATA$ACTION = 'INSERT'
        AND SOURCE.METADATA$ISUPDATE = 'FALSE';
    {%- endif -%}

    
{%- else -%}
    {{ log("  => No record(s) to merge input streams are empty " , info=True) }}
{%- endif -%}
{%- endmacro %}



{%- materialization incremental_stream, adapter='snowflake' -%}
    {% set original_query_tag = set_query_tag() %}

    {% set target_relation = this %}
    {%- set unique_key = config.get('unique_key') -%}
    {% set incremental_strategy = config.get('incremental_strategy') or 'default' %}
    {% set tmp_relation_type = dbt_snowflake_get_tmp_relation_type(incremental_strategy, unique_key, 'sql') %}
    {% set tmp_relation = make_temp_relation(target_relation).incorporate(type=tmp_relation_type) %}
    {%- set existing_relation = load_relation(target_relation) -%}

    -- setup
    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    -- `BEGIN` happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {#-- Create the MERGE --#}
    {%- set build_sql = incr_stream.merge_stream_sql(target_relation, sql, tmp_relation, unique_key) -%}

    {%- call statement('main') -%}
        {{ build_sql }}
    {%- endcall -%}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    -- `COMMIT` happens here
    {{ adapter.commit() }}

    {{ run_hooks(post_hooks, inside_transaction=False) }}
    {% do drop_relation_if_exists(tmp_relation) %}

    {% do unset_query_tag(original_query_tag) %}

    {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
