select collision_id, factor_1 as contributing_factor from {{ ref('stg_collisions') }} where factor_1 is not null and factor_1 != 'Unspecified'
union all
select collision_id, factor_2 from {{ ref('stg_collisions') }} where factor_2 is not null and factor_2 != 'Unspecified'
union all
select collision_id, factor_3 from {{ ref('stg_collisions') }} where factor_3 is not null and factor_3 != 'Unspecified'
union all
select collision_id, factor_4 from {{ ref('stg_collisions') }} where factor_4 is not null and factor_4 != 'Unspecified'
union all
select collision_id, factor_5 from {{ ref('stg_collisions') }} where factor_5 is not null and factor_5 != 'Unspecified'