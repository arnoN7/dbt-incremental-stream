{#-- Return stream name or source name in full-refresh mode --#}
{%- macro stream_source(source_name, table_name) -%}
    {{incr_stream.stream_input(table_name, 'source', source_name=source_name)}}
{%- endmacro -%}

{#-- Return stream name or ref name in full-refresh mode --#}
{%- macro stream_ref(model_name) -%}
    {{incr_stream.stream_input(model_name, 'ref')}}
{%- endmacro -%}

{%- macro stream_input(table_name, mode, source_name='') -%}
    {%- set input_model = none -%}
    {% set streams = [] %}
    {{ log("HELLO", info=True) }}
    {%- if config.get('streams') -%}
        {{ log('streams initialized' , info=True) }}
        {{ log(config.get('streams') , info=True) }}
        {% set streams = config.get('streams') %}
    {%- else -%}
        {{ log('streams reset' , info=True) }}
        {{- config.set('streams', streams) -}}
        {{ log(config.get('streams') , info=True) }}
    {%- endif -%} 
    {%- if mode == 'source' -%}
        {{ log(config.get('streams') , info=True) }}
        {{ log('stream added' , info=True) }}
        {{ streams.append([source_name, table_name]) | replace("None", "") }}
        {%- set input_model = source(source_name, table_name) -%}
    {%- else -%}
        {{ log(config.get('streams') , info=True) }}
        {{ log(streams , info=True) }}
        {{ streams.append([none, table_name]) | replace("None", "") }}
        {{ log('stream added' , info=True) }}
        {{ log(config.get('streams') , info=True) }}
        {{ log(streams , info=True) }}
        {%- set input_model = ref(table_name) -%}
    {%- endif -%}
    {{- config.set('streams', streams) -}}
    {%- set input_database = input_model.database  -%}
    {%- set input_schema = input_model.schema  -%}
    {{ log("schema "~input_schema~this.schema , info=True) }}
    {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}
    {%- if full_refresh_mode -%}
        {{input_model}}
    {%- else -%}
        {{input_model | replace(input_database, this.database) | replace(input_schema, this.schema) | replace(table_name, incr_stream.get_stream_name(this.table, table_name))}}
    {%- endif -%}
{%- endmacro -%}

{%- macro get_stream_name(table_name, source_table_name) -%}
{{- 'S_'~table_name~'_'~source_table_name -}}
{%- endmacro -%}
