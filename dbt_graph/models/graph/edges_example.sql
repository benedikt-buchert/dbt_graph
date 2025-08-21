{{
    config(
        materialized="table",
    )
}}

{% set identifier_array = [
    {
        "table_name": "foo_table",
        "identifiers": [
            {"id": "foo", "id_type": "foo"},
            {"id": "bar"},
            {"id": "baz"},
            {"id": "boo"},
        ],
    },
    {
        "table_name": "bar_table",
        "identifiers": [
            {"id": "foo", "id_type": "foo"},
            {"id": "bar"},
            {"id": "baz"},
            {"id": "boo"},
        ],
    }
] %}

with {{ prepare_edge_ctes(identifier_array) -}} {{- union_edge_ctes(identifier_array) }}

select *
from unioned
