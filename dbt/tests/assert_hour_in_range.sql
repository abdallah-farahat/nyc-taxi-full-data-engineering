Select * From {{ source('gold', 'agg_trips_hourly') }}
Where pickup_hour < 0 Or pickup_hour > 23