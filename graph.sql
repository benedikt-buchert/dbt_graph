{{ config(materialized="table", tags=["daily"]) }}

with recursive
    connected_components as (
        select
            source_node as node_id,
            source_node as front,
            target_node,
            1 as depth,
            [source_node] as visited_nodes
        from {{ ref("base_identity_resolution__reverse_edges") }}
        union all
        select
            cc.node_id,
            e.source_node as front,
            e.target_node,
            cc.depth + 1 as depth,
            array_concat(cc.visited_nodes, [e.source_node]) as visited_nodes
        from {{ ref("base_identity_resolution__reverse_edges") }} as e
        inner join connected_components as cc on e.source_node = cc.target_node
        where cc.depth < 100 and not e.target_node in unnest(cc.visited_nodes)
    ),

    walked as (
        select distinct node_id, front
        from connected_components
        union distinct
        select distinct node_id, target_node as front
        from connected_components
    ),

    unique_edges as (
        select distinct source_node, source_type
        from {{ ref("base_identity_resolution__reverse_edges") }}
    ),

    component as (
        select
            w.node_id,
            e.source_type as node_type,
            to_hex(md5(min(w.front))) as component_id
        from walked as w
        left join unique_edges as e on w.node_id = e.source_node
        group by 1, 2
    )

select *
from component
