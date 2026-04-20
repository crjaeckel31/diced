with segments as (
    select * from {{ ref('stg_street_segments') }}
),

final as (
    select
        segment_id,
        roadway_type,
        geom,
        number_of_travel_lanes,
        speed_limit,
        one_way,
        full_street_name,
        borough,
        bike_lane
    from segments
    where roadway_type in ('street', 'highway', 'ramp', 'alley', 'driveway') -- Only consider segments where vehicles are likely to be present
)

select * from final