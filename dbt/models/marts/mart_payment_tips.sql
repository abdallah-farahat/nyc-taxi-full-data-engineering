-- Q2: How do payment type and tipping differ?
-- Tips are recorded for card payments only, so the tip columns use card trips only.
Select
    d.year_month,
    l.borough,
    p.payment_name,
    sum(a.trip_count) As trips,
    sum(a.card_trip_count) As card_trips,
    cast(sum(a.card_tip_sum) As Decimal(18, 2)) As card_tip_total,
    cast(sum(a.card_tip_sum) / nullif(sum(a.card_trip_count), 0) As Decimal(10, 2)) As avg_tip_per_card_trip
From {{ source('gold', 'agg_trips_daily') }} a
Join {{ source('gold', 'dim_date') }} d On d.date_key = a.pickup_date_key
Join {{ source('gold', 'dim_location') }} l On l.location_key = a.pu_location_key
Join {{ source('gold', 'dim_payment') }} p On p.payment_key = a.payment_key
Group By d.year_month, l.borough, p.payment_name