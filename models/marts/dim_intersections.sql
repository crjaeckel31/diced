  with endpoints as (
      select segment_id, full_street_name, borough, ST_StartPoint(geom) as pt
      from {{ ref('dim_street_segments') }}
      union all
      select segment_id, full_street_name, borough, ST_EndPoint(geom) as pt
      from {{ ref('dim_street_segments') }}
  ),

  grouped as (
      select
          ST_X(pt) as lon,
          ST_Y(pt) as lat,
          any_value(borough) as borough, -- always one borough per intersection
          list(distinct full_street_name) as street_names
      from endpoints
      group by ST_X(pt), ST_Y(pt)
      having count(distinct full_street_name) >= 2   -- drops dead ends
          and ST_X(pt) is not null
          and ST_Y(pt) is not null
  ),

  final as (
      select
          md5(concat(lon::varchar, '|', lat::varchar)) as intersection_id,
          lon,
          lat,
          borough,
          street_names
      from grouped
  )

  select * from final