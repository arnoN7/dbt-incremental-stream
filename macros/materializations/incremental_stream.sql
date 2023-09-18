{#-- ###################################################### --#}
{#-- Merge of stream to the target                          --#}
{#-- Main parameters :                                      --#}
{#--    1) sql = SELECT statement with source or ref tables --#}
{#--    2) src_table = source or ref table  		        --#}
{#--    3) unique_key = PK used for the merge               --#}
{#--    3) sql = SQL used for the transformation            --#}
{#-- ###################################################### --#}

{%- macro merge_stream_sql(target_relation, sql, tmp_relation, unique_key) -%}
    {%- call statement('create_tmp_relation') -%}
        {{ snowflake__create_view_as_with_temp_flag(tmp_relation, sql, True) }}
    {%- endcall -%}

    {%- set columns = adapter.get_columns_in_relation(tmp_relation) | rejectattr('name', 'equalto', 'METADATA$ACTION')
                | rejectattr('name', 'equalto', 'METADATA$ISUPDATE')
                | rejectattr('name', 'equalto', 'METADATA$ROW_ID')
                | list %}
    {%- if unique_key -%}
        MERGE INTO  {{ target_relation }} TARGET
        USING {{tmp_relation}} SOURCE
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
        SOURCE.{{col.name}}{%- if not loop.last %},{% endif %}{% endfor %} FROM {{tmp_relation}} SOURCE WHERE SOURCE.METADATA$ACTION = 'INSERT' 
        AND SOURCE.METADATA$ISUPDATE = 'FALSE';
    {%- endif -%}
{%- endmacro %}



{%- materialization incremental_stream, adapter='snowflake' -%}
    {% set target_relation = this %}
    {%- set unique_key = config.get('unique_key') -%}
    {% set incremental_strategy = config.get('incremental_strategy') or 'default' %}
    {% set tmp_relation_type = dbt_snowflake_get_tmp_relation_type(incremental_strategy, unique_key, 'sql') %}
    {% set tmp_relation = make_temp_relation(this).incorporate(type=tmp_relation_type) %}
    {%- set existing_relation = load_relation(this) -%}
    {%- set src_list = config.get('src_list') -%}
    {% set grant_config = config.get('grants') %}
    {% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}
    {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}
    {% set relation_exists = load_relation(target_relation) is not none %}
    {%- set is_refresh = full_refresh_mode or not relation_exists -%}
    {%- set streams = [] -%}
    {%- set build_sql = "" -%}
    
    

    -- setup
    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    
    -- `BEGIN` happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {#-- 1. Collect the streams to create or use as source(s)  --#}
    {%- set build_sql_stream -%}
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
            {#-- Drop existing stream(s) in full-refresh mode  --#}
            {% if is_refresh -%} 
                DROP STREAM IF EXISTS {{target_stream}};
            {% endif %}
            {#-- CREATE OBJECTS (STREAM, TABLE) IF NOT EXISTS  --#}
            CREATE STREAM IF NOT EXISTS {{target_stream}} ON TABLE {{source_table}};
        {% endfor %}
    {%- endset -%}

    {#-- 2. Check if there is data in streams if target relation not exist or not full-refresh --#}
    {%- set is_data_in_streams = False -%}
    
    {%- if is_refresh %} 
        {%- set build_sql -%}
        CREATE OR REPLACE TABLE {{ target_relation }} AS SELECT * FROM ({{sql}});
        {%- endset -%}
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
                    {%- set is_data_in_streams = False -%}
            {%- endif -%}
    {%- endif -%}

    {#-- 3. Execute Merge only if new records in stream  --#}
    {%- if is_data_in_streams  -%}
    {{ log("  => Record(s) to merge" , info=True) }}
        {#-- Create the MERGE (if unique Key) or INSERT (without unique key) --#}
        {%- set build_sql = incr_stream.merge_stream_sql(target_relation, sql, tmp_relation, unique_key) -%}
    {%- else -%}
        {{ log("  => No record(s) to merge input streams are empty " , info=True) }}
    {%- endif -%}

    {%- call statement('main') -%}
        {{ build_sql_stream }}
        {{ build_sql }}
    {%- endcall -%}

    -- `COMMIT` happens here
    {{ adapter.commit() }}
    {{ run_hooks(post_hooks, inside_transaction=False) }}
    {% if not is_refresh %}
        {% do drop_relation_if_exists(tmp_relation) %}
    {% endif %}

    {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
