with source as (
    select *
    from {{ source('raw', 'pavement_ratings') }}
),

renamed as (
    select
        ST_GeomFromText(the_geom) as geom,
        "OFTCode"::varchar as oft_code,
        nullif("SystemRating", 0.0)::double as system_rating,
        case
            when SystemRating >= 9 then 'excellent'
            when SystemRating >= 7 then 'good'
            when SystemRating >= 5 then 'fair'
            when SystemRating >= 3 then 'poor'
            when SystemRating > 0  then 'very_poor'
            when SystemRating = 0 then null
            else null
        end as rating_tier,
        "NonRatingReason"::varchar as non_rating_reason,
        "BoroughName"::varchar as borough,
        "OnStreetName"::varchar as on_street_name,
        "Road_Type"::varchar as road_type,
        "InspectionTime"::timestamp as inspection_time
    from source
)

select *
from renamed