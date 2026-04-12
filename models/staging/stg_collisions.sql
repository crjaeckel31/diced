with source as (
    select *
    from {{ source('raw', 'collisions') }}
),

renamed as (
    select
        "COLLISION_ID"::integer as collision_id,
        strptime("CRASH DATE", '%m/%d/%Y')::date as crash_date,
        "CRASH TIME"::varchar as crash_time,
        "LATITUDE"::double as lat,
        "LONGITUDE"::double as lon,
        "BOROUGH"::varchar as borough,
        "CONTRIBUTING FACTOR VEHICLE 1"::varchar as factor_1,
        "CONTRIBUTING FACTOR VEHICLE 2"::varchar as factor_2,
        "CONTRIBUTING FACTOR VEHICLE 3"::varchar as factor_3,
        "CONTRIBUTING FACTOR VEHICLE 4"::varchar as factor_4,
        "CONTRIBUTING FACTOR VEHICLE 5"::varchar as factor_5,
        "NUMBER OF PERSONS INJURED"::integer as persons_injured,
        "NUMBER OF PERSONS KILLED"::integer as persons_killed
    from source
    where "LATITUDE" is not null
      and "LONGITUDE" is not null
)

select *
from renamed