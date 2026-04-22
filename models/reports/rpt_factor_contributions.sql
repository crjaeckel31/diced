with

crashes as (
    select
        collision_id,
        segment_id
    from {{ ref('fct_crashes_enriched') }}
),

factors as (
    select
        collision_id,
        contributing_factor
    from {{ ref('fct_crash_factors') }}
),

segments as (
    select
        segment_id,
        roadway_type,
        number_of_travel_lanes,
        speed_limit,
        one_way,
        bike_lane
    from {{ ref('dim_street_segments') }}
),

joined as (
    select
        crashes.collision_id,
        crashes.segment_id,
        factors.contributing_factor,
        segments.roadway_type,
        segments.number_of_travel_lanes,
        segments.speed_limit,
        segments.one_way,
        segments.bike_lane
    from crashes
    inner join factors on crashes.collision_id = factors.collision_id
    inner join segments on crashes.segment_id = segments.segment_id
),

-- segment counts per attribute value (denominator for crash rates)
bike_lane_segment_counts as (
    select cast(bike_lane as varchar) as attribute_value, count(*) as segment_count
    from segments
    group by bike_lane
),

speed_limit_segment_counts as (
    select cast(speed_limit as varchar) as attribute_value, count(*) as segment_count
    from segments
    group by speed_limit
),

road_class_segment_counts as (
    select roadway_type as attribute_value, count(*) as segment_count
    from segments
    group by roadway_type
),

-- crash counts per (attribute_value, contributing_factor)
bike_lane_stats as (
    select
        'bike_lane' as attribute_name,
        cast(j.bike_lane as varchar) as attribute_value,
        j.contributing_factor,
        count(distinct j.collision_id) as crash_count,
        sc.segment_count
    from joined j
    inner join bike_lane_segment_counts sc on cast(j.bike_lane as varchar) = sc.attribute_value
    group by j.bike_lane, j.contributing_factor, sc.segment_count
),

speed_limit_stats as (
    select
        'speed_limit' as attribute_name,
        cast(j.speed_limit as varchar) as attribute_value,
        j.contributing_factor,
        count(distinct j.collision_id) as crash_count,
        sc.segment_count
    from joined j
    inner join speed_limit_segment_counts sc on cast(j.speed_limit as varchar) = sc.attribute_value
    group by j.speed_limit, j.contributing_factor, sc.segment_count
),

road_class_stats as (
    select
        'road_class' as attribute_name,
        j.roadway_type as attribute_value,
        j.contributing_factor,
        count(distinct j.collision_id) as crash_count,
        sc.segment_count
    from joined j
    inner join road_class_segment_counts sc on j.roadway_type = sc.attribute_value
    group by j.roadway_type, j.contributing_factor, sc.segment_count
),

unioned as (
    select * from bike_lane_stats
    union all
    select * from speed_limit_stats
    union all
    select * from road_class_stats
),

-- overall crash rate across all geocoded crashes and all segments
baseline as (
    select
        count(distinct collision_id)::float
        / (select count(distinct segment_id) from segments) as baseline_rate
    from joined
),

final as (
    select
        u.attribute_name,
        u.attribute_value,
        u.contributing_factor,
        u.crash_count,
        u.segment_count,
        round(u.crash_count::float / u.segment_count, 4) as crash_rate,
        round(b.baseline_rate, 4) as baseline_rate,
        round((u.crash_count::float / u.segment_count) / b.baseline_rate, 2) as rate_ratio
    from unioned u
    cross join baseline b
    order by attribute_name, rate_ratio desc
)

select * from final