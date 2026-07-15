{#-- Return stream name or source name in full-refresh mode --#}
{%- macro stream_source(source_name, table_name, stream_type='STANDARD') -%}
    {{stream_input(table_name, 'source', stream_type, source_name=source_name)}}
{%- endmacro -%}

{#-- Return stream name or ref name in full-refresh mode --#}
{%- macro stream_ref(model_name, stream_type='STANDARD') -%}
    {{stream_input(model_name, 'ref', stream_type)}}
{%- endmacro -%}

{%- macro stream_input(table_name, mode, stream_type, source_name='') -%}
    {%- if mode == 'source' -%}
        {%- set input_model = source(source_name, table_name) -%}
    {%- else -%}
        {%- set input_model = ref(table_name) -%}
    {%- endif -%}
    {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}
    {%- set relation_exists = load_relation(this) is not none -%}
    {%- if full_refresh_mode or not relation_exists -%}
        {{input_model}}
    {%- else -%}
        {{ this.include(identifier=false) | string + '.' + get_stream_name(this.identifier, input_model.identifier) }}
    {%- endif -%}
{%- endmacro -%}

{%- macro get_stream_name(table_name, source_table_name) -%}
{%- set table_name = table_name | string -%}
{%- set source_table_name = source_table_name | string -%}
{%- set is_quoted = false -%}
{%- if '"' in table_name -%}
    {%- set is_quoted = true -%}
    {%- set table_name = table_name[1:-1]  -%}
{%- endif -%}
{%- if '"' in source_table_name -%}
    {%- set is_quoted = true -%}
    {%- set source_table_name = source_table_name[1:-1]  -%}
{%- endif -%}
{%- if is_quoted == true -%}
    {{- '"S_'~table_name~'_'~source_table_name~'"' -}}
{%- else -%}
    {{- 'S_'~table_name~'_'~source_table_name -}}
{%- endif -%}
{%- endmacro -%}

{%- macro get_stream_metadata_columns(alias) -%}
{% set final_alias = '' -%}
{% if alias -%}
    {% set final_alias = alias + '.' -%}
{% endif -%}
{%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}
{%- set relation_exists = load_relation(this) is not none -%}
{%- if full_refresh_mode or not relation_exists -%}
{# No metadata stream for full refresh #}
{%- else -%}
,{{ final_alias }}METADATA$ACTION, {{ final_alias }}METADATA$ISUPDATE, {{ final_alias }}METADATA$ROW_ID
{%- endif -%}
{%- endmacro -%}