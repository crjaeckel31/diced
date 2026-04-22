with

crashes as (
    select
        collision_id,
        segment_id,
        persons_killed,
        persons_injured
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
        full_street_name,
        roadway_type,
        speed_limit,
        bike_lane
    from {{ ref('dim_street_segments') }}
),

-- attach severity score per crash; property-only crashes score 0
scored as (
    select
        collision_id,
        segment_id,
        (persons_killed * 5) + (persons_injured * 1) as severity_score
    from crashes
),

-- one row per (crash, factor) with severity score
joined as (
    select
        s.collision_id,
        s.segment_id,
        s.severity_score,
        f.contributing_factor
    from scored s
    inner join factors f on s.collision_id = f.collision_id
),

-- total severity and crash count per segment (exclude property-only)
segment_stats as (
    select
        segment_id,
        count(distinct collision_id)  as crash_count,
        sum(severity_score)           as total_severity_score
    from joined
    where severity_score > 0
    group by segment_id
),

-- rank factors within each segment by how many crashes cite them
factor_ranks as (
    select
        segment_id,
        contributing_factor,
        count(distinct collision_id) as factor_crash_count,
        rank() over (partition by segment_id order by count(distinct collision_id) desc) as factor_rank
    from joined
    group by segment_id, contributing_factor
),

-- keep only the top 3 factors per segment
top_factors as (
    select
        segment_id,
        contributing_factor,
        factor_crash_count,
        factor_rank
    from factor_ranks
    where factor_rank <= 3
),

final as (
    select
        ss.segment_id,
        seg.full_street_name,
        seg.roadway_type,
        seg.speed_limit,
        seg.bike_lane,
        ss.crash_count,
        ss.total_severity_score,
        tf.factor_rank,
        tf.contributing_factor,
        tf.factor_crash_count
    from segment_stats ss
    inner join segments seg on ss.segment_id = seg.segment_id
    inner join top_factors tf on ss.segment_id = tf.segment_id
    order by ss.total_severity_score desc, ss.segment_id, tf.factor_rank
)

select * from final
