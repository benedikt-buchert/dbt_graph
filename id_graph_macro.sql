{%- macro prepare_edge_ctes(tables_array) -%}
    {%- for table in tables_array %}
        {%- for identifier in table.identifiers -%}
            {%- set index_1 = loop.index -%}
            {%- for identifier_2 in table.identifiers -%}
                {%- set index_2 = loop.index -%}
                {{ log(identifier) }}
                {%- if index_2 > index_1 -%}
                    {%- set source_node_type = identifier.get(
                        "id_type", identifier.id
                    ) -%}
                    {%- set target_node_type = identifier_2.get(
                        "id_type", identifier_2.id
                    ) -%}

                    {{ table.table_name }}_{{ identifier.id }}_{{ identifier_2.id }}_edge
                    as (
                        select
                            {{ identifier.id }} as source_node,
                            {{ identifier_2.id }} as target_node,
                            '{{ source_node_type }}' as source_type,
                            '{{ target_node_type }}' as target_type,
                            to_hex(
                                md5(concat({{ identifier.id }}, {{ identifier_2.id }}))
                            ) as edge_id
                        from {{ ref(table.table_name) }}
                        where
                            {{ identifier.id }} is not null
                            and {{ identifier_2.id }} is not null
                    ),
                {%- endif -%}
            {%- endfor -%}
        {%- endfor -%}

    {%- endfor -%}
{%- endmacro -%}

{%- macro union_edge_ctes(tables_array) -%}
    {% set unioned_queries = [] %}

    {%- for table in tables_array -%}
        {%- for identifier in table.identifiers -%}
            {% set index_1 = loop.index %}
            {%- for identifier_2 in table.identifiers -%}
                {% set index_2 = loop.index %}
                {%- if index_2 > index_1 -%}
                    {% set query = (
                        "select * from "
                        ~ table.table_name
                        ~ "_"
                        ~ identifier.id
                        ~ "_"
                        ~ identifier_2.id
                        ~ "_edge"
                    ) %}
                    {% do unioned_queries.append(query) %}
                {% endif %}
            {% endfor %}
        {%- endfor -%}
    {%- endfor -%}

    unioned as ({{ unioned_queries | join(" union all\n") }})
{%- endmacro -%}
