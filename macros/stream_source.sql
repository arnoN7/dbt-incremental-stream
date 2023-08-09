{#-- Return stream name or source name in full-refresh mode --#}
{%- macro stream_source(source_name, table_name) -%}
    {{incr_stream.stream_input(table_name, 'source', source_name=source_name)}}
{%- endmacro -%}

{#-- Return stream name or ref name in full-refresh mode --#}
{%- macro stream_ref(model_name) -%}
    {{incr_stream.stream_input(model_name, 'ref')}}
{%- endmacro -%}

{%- macro stream_input(table_name, mode, source_name='') -%}
    {%- set input_model = None -%}
    {{- config.set('src_table', table_name) -}}
    {%- if mode == 'source' -%}
        {{- config.set('src', source_name) -}}
        {%- set input_model = source(source_name, table_name) -%}
    {%- else -%}
        {%- set input_model = ref(table_name) -%}
    {%- endif -%}
    {%- set input_database = input_model.database  -%}
    {%- set input_schema = input_model.schema  -%}
    {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}
    {% set relation_exists = load_relation(this) is not none %}
    {%- if full_refresh_mode or not relation_exists -%}
        {{input_model}}
    {%- else -%}
        {{input_model | replace(input_database, this.database) | replace(input_schema, this.schema) | replace(table_name, incr_stream.get_stream_name(this.table, table_name))}}
    {%- endif -%}
{%- endmacro -%}

{%- macro get_stream_name(table_name, source_table_name) -%}
{{- 'S_'~table_name~'_'~source_table_name -}}
{%- endmacro -%}

{%- macro get_stream_metadata_columns(alias) -%}
{% set final_alias = '' -%}
{% if alias -%}
    {% set final_alias = alias + '.' -%}
{% endif -%}
{%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}
{% set relation_exists = load_relation(this) is not none %}
{%- if full_refresh_mode or not relation_exists -%}
{# No metadata stream for full refresh #} 
{%- else -%}
,{{ final_alias }}METADATA$ACTION, {{ final_alias }}METADATA$ISUPDATE, {{ final_alias }}METADATA$ROW_ID
{%- endif -%}
{%- endmacro -%}