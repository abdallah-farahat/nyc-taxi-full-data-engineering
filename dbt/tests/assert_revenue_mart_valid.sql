Select * From {{ ref('mart_revenue_monthly') }}
Where total_revenue < 0 Or avg_fare < 0 Or avg_fare > 1000 Or trips <= 0