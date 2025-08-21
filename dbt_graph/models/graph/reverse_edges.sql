with
    edges as (select * from {{ ref("base_identity_resolution__edges") }}),

    distinct_edges as (select distinct * from edges),

    reversed as (
        select
            target_node as source_node,
            source_node as target_node,
            target_type as source_type,
            source_type as target_type,
            concat(edge_id, "_reverse") as edge_id
        from distinct_edges
        union all
        select *
        from distinct_edges
    )

select *
from reversed
