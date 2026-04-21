with crashes as (                                                                                   
      select * from {{ ref('stg_collisions') }}
  ),
                                                                                                                                                                     
  geocoded as (
      select collision_id, crash_point, segment_id                                                                                                                   
      from {{ ref('int_collisions_geocoded') }}
  ),                                                                                                  
                                         
  final as (
      select
          c.collision_id,
          c.crash_date,                                                                                                                                              
          c.crash_time,
          g.crash_point,                                                                                                                                             
          g.segment_id,                                                                                        
          c.persons_injured,             
          c.persons_killed                                                                                              
      from crashes c
      inner join geocoded g using (collision_id)                                                                                                                     
  )                            
                                                                                                      
  select * from final