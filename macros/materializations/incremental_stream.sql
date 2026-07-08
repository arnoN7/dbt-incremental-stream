{%- macro merge_stream_sql(target_relation, sql, tmp_relation, unique_key, src_list) -%}

{% do log("Running merge_stream_sql macro...", info=True) %}

{%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}
{%- set target_relation = this -%}
{% set grant_config = config.get('grants') %}
{% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}
{%- set streams = [] -%}
{%- set materialization = 'table' -%}

{#-- Drop Target Table in full-refresh mode --#}
{% if full_refresh_mode -%}
    DROP TABLE IF EXISTS {{ target_relation }};
{% endif %}

{% for src in src_list %}
    {%- if src['mode'] == 'source' -%}
        {%- set source_table = load_relation(source(src['source_name'], src['table_name'])) -%}
        {%- set source_table_raw = source(src['source_name'], src['table_name']) -%}
    {%- else -%}
        {%- set source_table = load_relation(ref(src['table_name'])) -%}
        {%- set source_table_raw = ref(src['table_name']) -%}
    {%- endif -%}
    {%- if source_table is not none and source_table.is_view -%}
        {%- set materialization = 'view' -%}
    {%- endif -%}
    {%- set target_stream = target_relation.database | upper
        ~ '.' ~ target_relation.schema | upper
        ~ '.' ~ get_stream_name(target_relation.identifier, source_table_raw.identifier) -%}
    {%- set _ = streams.append(target_stream) %}
    {{ log("STREAM NAME: " ~ target_stream, info=True) }}

    {#-- Drop Stream in full-refresh mode --#}
    {% if full_refresh_mode -%}
        DROP STREAM IF EXISTS {{ target_stream }};
    {% endif %}

    {#-- CREATE OBJECTS (STREAM, TABLE) IF NOT EXISTS --#}
    {%- set stream_type = src.get('stream_type', 'STANDARD') | upper -%}
    CREATE STREAM IF NOT EXISTS {{ target_stream }} ON {{ materialization }} {{ source_table_raw }}
        {%- if var('TIMESTAMP', False) %} AT (TIMESTAMP => TO_TIMESTAMP_TZ('{{ var('TIMESTAMP') }}', 'yyyy-mm-dd hh24:mi:ss')){%- endif -%}
        {%- if stream_type in ['APPEND_ONLY', 'INSERT_ONLY'] %} {{ stream_type }} = TRUE{%- endif -%};
{% endfor %}


{%- set relation_exists = load_relation(target_relation) is not none -%}
{{ log("Relation_exists = " ~ relation_exists, info=True) }}
{{ log("Full_refresh_mode = " ~ full_refresh_mode, info=True) }}
{%- if full_refresh_mode or not relation_exists %}
    CREATE TABLE IF NOT EXISTS {{ target_relation }} AS SELECT * FROM ({{ sql }});
{%- else -%}

    {#-- Check the presence of records to merge --#}
    {%- set is_data_in_streams -%}
        select ({% for stream in streams %}
        system$stream_has_data('{{ stream }}'){%- if not loop.last %} OR {% endif %}
        {% endfor %})
    {%- endset -%}
    {%- set results = run_query(is_data_in_streams) -%}
    {%- set is_data_in_streams = false -%}

    {%- if execute -%}
        {%- if results | length > 0 -%}
            {%- set is_data_in_streams = results.columns[0].values()[0] -%}
        {%- else -%}
            {{ log("  => Expected streams not found", info=True) }}
        {%- endif -%}
    {%- endif -%}

    {#-- Execute Merge only if new records in stream --#}
    {%- if is_data_in_streams -%}
        {{ log("  => record(s) to merge", info=True) }}
        {%- call statement('create_tmp_relation') -%}
            {{ snowflake__create_view_as_with_temp_flag(tmp_relation, sql, True) }}
        {%- endcall -%}

        {%- set columns = adapter.get_columns_in_relation(tmp_relation)
                | rejectattr('name', 'equalto', 'METADATA$ACTION')
                | rejectattr('name', 'equalto', 'METADATA$ISUPDATE')
                | rejectattr('name', 'equalto', 'METADATA$ROW_ID')
                | list %}

        {%- if unique_key -%}
        MERGE INTO {{ target_relation }} TARGET
        USING (
            SELECT *
            FROM {{ tmp_relation }}
            QUALIFY
                NOT (
                    COUNT(CASE WHEN METADATA$ACTION = 'DELETE' AND METADATA$ISUPDATE = FALSE THEN 1 END)
                        OVER (PARTITION BY {% for key in unique_key %}{{ key }}{%- if not loop.last %}, {% endif %}{% endfor %}) > 0
                    AND
                    COUNT(CASE WHEN METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = FALSE THEN 1 END)
                        OVER (PARTITION BY {% for key in unique_key %}{{ key }}{%- if not loop.last %}, {% endif %}{% endfor %}) > 0
                    AND
                    METADATA$ISUPDATE = FALSE
                )
        ) SOURCE
        ON ({% for key in unique_key %}
            TARGET.{{ key }} = SOURCE.{{ key }}{%- if not loop.last %} AND {% endif %}{% endfor %}
        )
        WHEN MATCHED AND SOURCE.METADATA$ACTION = 'DELETE' AND SOURCE.METADATA$ISUPDATE = FALSE THEN
            UPDATE SET
                TARGET.is_deleted = TRUE,
                TARGET.stream_updated_at = CURRENT_TIMESTAMP()
        WHEN MATCHED AND SOURCE.METADATA$ACTION = 'INSERT' THEN
            UPDATE SET {%- for col in columns %}
            TARGET.{{ col.name }} = SOURCE.{{ col.name }}{%- if not loop.last %},{% endif %}{% endfor %}
        WHEN NOT MATCHED AND SOURCE.METADATA$ACTION = 'INSERT' THEN INSERT
            ({% for col in columns %}
            {{ col.name }}{%- if not loop.last %},{% endif %}{% endfor %}
            )
            VALUES
            ({% for col in columns %}
            SOURCE.{{ col.name }}{%- if not loop.last %},{% endif %}{% endfor %}
            );
        {%- else -%}
            INSERT INTO {{ target_relation }}
            SELECT {% for col in columns %}
            SOURCE.{{ col.name }}{%- if not loop.last %},{% endif %}{% endfor %}
            FROM {{ tmp_relation }} SOURCE
            WHERE SOURCE.METADATA$ACTION = 'INSERT'
            AND SOURCE.METADATA$ISUPDATE = 'FALSE';
        {%- endif -%}

    {%- else -%}
        {{ log("  => No record(s) to merge, input streams are empty", info=True) }}
    {%- endif -%}

{%- endif -%}
{%- endmacro %}


{%- materialization incremental_stream, adapter='snowflake' -%}

    {%- set target_relation = this -%}
    {%- set unique_key = config.get('unique_key') -%}
    {% set incremental_strategy = config.get('incremental_strategy') or 'default' %}
    {% set tmp_relation_type = dbt_snowflake_get_tmp_relation_type(incremental_strategy, unique_key, 'sql') %}
    {% set tmp_relation = make_temp_relation(target_relation).incorporate(type=tmp_relation_type) %}
    {%- set existing_relation = load_relation(target_relation) -%}

    {#-- ------------------------------------------------------------------ --#}
    {#-- Fusion-compatible: derive src_list from graph node                 --#}
    {#-- ------------------------------------------------------------------ --#}
    {%- set node = graph.nodes.get(model.unique_id) -%}
    {%- set src_list = [] -%}

    {%- if node -%}
        {#-- Check meta first for any explicit src_list override --#}
        {%- set meta_src_list = config.meta_get('src_list') -%}
        {%- if meta_src_list -%}
            {%- set src_list = meta_src_list -%}
            {{ log("MATERIALIZATION: using meta src_list override", info=True) }}
        {%- else -%}
            {#-- Derive from graph node dependencies --#}
            {%- for dep_id in node.depends_on.nodes -%}
                {%- if dep_id.startswith('source.') -%}
                    {%- set source_node = graph.sources.get(dep_id) -%}
                    {%- if source_node -%}
                        {%- set stream_type = config.meta_get('stream_type') or 'STANDARD' -%}
                        {%- do src_list.append({
                            'mode': 'source',
                            'stream_type': stream_type,
                            'source_name': source_node.source_name,
                            'table_name': source_node.name
                        }) -%}
                    {%- endif -%}
                {%- elif dep_id.startswith('model.') -%}
                    {%- set ref_node = graph.nodes.get(dep_id) -%}
                    {%- if ref_node -%}
                        {%- set stream_type = config.meta_get('stream_type') or 'STANDARD' -%}
                        {%- do src_list.append({
                            'mode': 'ref',
                            'stream_type': stream_type,
                            'table_name': ref_node.name
                        }) -%}
                    {%- endif -%}
                {%- endif -%}
            {%- endfor -%}
            {{ log("MATERIALIZATION: derived src_list from graph = " ~ src_list, info=True) }}
        {%- endif -%}
    {%- endif -%}

    {%- if src_list | length == 0 -%}
        {{ exceptions.raise_compiler_error(
            "incremental_stream: could not derive src_list for model " ~ this ~
            ". Ensure the model calls stream_source() or stream_ref() in its SQL, " ~
            "or declare src_list explicitly under config(meta={'src_list': [...]})."
        ) }}
    {%- endif -%}

    -- setup
    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    -- `BEGIN` happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {#-- Create the MERGE --#}
    {%- set build_sql = merge_stream_sql(target_relation, sql, tmp_relation, unique_key, src_list) -%}

    {%- call statement('main') -%}
        {{ build_sql }}
    {%- endcall -%}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    -- `COMMIT` happens here
    {{ adapter.commit() }}

    {{ run_hooks(post_hooks, inside_transaction=False) }}
    {% do drop_relation_if_exists(tmp_relation) %}

    {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}