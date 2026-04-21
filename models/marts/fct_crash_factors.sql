 with factors as (
     select * from {{ ref('int_crash_factors_unpivoted') }}
 ),

 final as (
     select distinct
         collision_id,
         contributing_factor
     from factors
 )

 select * from final