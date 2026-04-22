with source as (
    select *
    from {{ source('raw', 'street_segments') }}
),

renamed as (
    select
        "physicalid"::integer as segment_id,
        geom,
        case rw_type::integer              
            when 1 then 'street'                                                                      
            when 2 then 'highway'
            when 3 then 'bridge'                                                                      
            when 4 then 'tunnel'        
            when 5 then 'boardwalk'        
            when 6 then 'path_trail'                                                                  
            when 7 then 'step_street'
            when 8 then 'driveway'                                                                    
            when 9 then 'ramp'          
            when 10 then 'alley'                                                                      
            when 11 then 'unknown'
            when 12 then 'non_physical'                                                               
            when 13 then 'u_turn'                                                                     
            when 14 then 'ferry_route'     
        end as roadway_type,
        "number_tra"::integer as number_of_travel_lanes,
        "posted_spe"::integer as speed_limit,
        case trafdir::varchar
            when 'FT' then true -- With
            when 'TF' then true -- Against
            when 'TW' then false -- Two-way
            when 'NV' then null -- Non-vehicular
            else null
        end as one_way,
        "full_stree"::varchar as full_street_name,
        case boroughcod::integer                                                                            
            when 1 then 'manhattan'                                                                                                                                        
            when 2 then 'bronx'         
            when 3 then 'brooklyn'                                                                          
            when 4 then 'queens'                                                                                                                                           
            when 5 then 'staten_island'
        end as borough,
        case bike_lane::integer
            when 1  then 'protected'
            when 2  then 'painted'
            when 3  then 'sharrow'
            when 4  then 'link'
            when 5  then 'protected_and_painted'
            when 6  then 'painted_and_sharrow'
            when 7  then 'stairs'
            when 8  then 'protected_and_sharrow'
            when 9  then 'painted_and_protected'
            when 10 then 'sharrow_and_protected'
            when 11 then 'sharrow_and_painted'
            else null
        end as bike_lane
    from source
    where physicalid is not null
)

select *
from renamed
qualify row_number() over (partition by segment_id order by segment_id) = 1