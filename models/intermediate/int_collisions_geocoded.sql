with crashes as (
    select collision_id, lat, lon, ST_Point(lon, lat) as crash_point
    from {{ ref('stg_collisions') }}
),

segments as (
    select segment_id, roadway_type, geom
    from {{ ref('stg_street_segments') }}
    where roadway_type in ('street', 'highway', 'ramp', 'alley', 'driveway') -- Only consider segments where vehicles are likely to be present
)

select
    c.collision_id,
    c.crash_point,
    s.segment_id,
    s.roadway_type
from crashes c -- We alias crashes as c and segments as s. with a cross join you need to distinguish which table each column comes from
cross join segments s
where ST_DWithin(c.crash_point, s.geom, 0.001)  -- ~100m in degrees. was too expensive to calculate distance for all segments, so we filter to only those within a reasonable distance first
qualify row_number() over (
    partition by c.collision_id
    order by ST_Distance(c.crash_point, s.geom) -- rank segments from closest to farthest
) = 1 -- keep only the closest one