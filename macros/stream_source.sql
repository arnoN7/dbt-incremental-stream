{%- macro stream_source(source_name, table_name) -%}
    {{- config.set('src', source_name) -}}
    {{- config.set('src_table', table_name) -}}
    {%- set source_database = source(source_name, table_name).database  -%}
    {%- set source_schema = source(source_name, table_name).schema  -%}
    {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}
    {%- if full_refresh_mode -%}
        {{source(source_name, table_name)}}
    {%- else -%}
        {{source(source_name, table_name) | replace(source_database, this.database) | replace(source_schema, this.schema) | replace(table_name, 'S_'~this.table )}}
    {%- endif -%}
{%- endmacro -%}

{%- macro stream_ref(model_name) -%}
    {{- config.set('src_table', model_name) -}}
    {%- set ref_database = ref(model_name).database  -%}
    {%- set ref_schema = ref(model_name).schema  -%}
    {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}
    {%- if full_refresh_mode -%}
        {{ref(model_name)}}
    {%- else -%}
        {{ref(model_name) | replace(ref_database, this.database) | replace(ref_schema, this.schema) | replace(model_name, 'S_'~this.table )}}
    {%- endif -%}
{%- endmacro -%}