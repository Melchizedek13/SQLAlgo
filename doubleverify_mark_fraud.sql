/*
   There is a table with rules "analyst_rules", based on which it is necessary to mark rows as fraud
     in the second table "pre_processed_fact_impressions".
       The second table "pre_processed_fact_impressions" is a pre-calculated aggregate with
         a minimum time granularity of up to an hour.
*/
-- https://dbfiddle.uk/CoSiNiXI - use `lag()` and cumulative sum

with
analyst_rules(site, apply_rule, num_of_events, hours_frame) as (values
   ('site1.com', 'fraud', 6, 3),
   ('site2.com', 'fraud', 20, 4)
),
pre_processed_fact_impressions(user_id, site, num_of_events, ts_hour) as (values
   (1, 'site1.com', 1, timestamp'2025-01-08 01:00:00'),
   (1, 'site1.com', 1, timestamp'2025-01-08 02:00:00'),
   (1, 'site1.com', 1, timestamp'2025-01-08 03:00:00'),
   (1, 'site1.com', 2, timestamp'2025-01-08 07:00:00'),
   (1, 'site1.com', 3, timestamp'2025-01-08 08:00:00'),
   (1, 'site1.com', 1, timestamp'2025-01-08 09:00:00'),
   (1, 'site1.com', 6, timestamp'2025-01-08 15:00:00'),
   (2, 'site2.com', 6, timestamp'2025-01-09 00:00:00'),
   (2, 'site2.com', 6, timestamp'2025-01-09 01:00:00'),
   (2, 'site2.com', 6, timestamp'2025-01-09 02:00:00'),
   (2, 'site2.com', 1, timestamp'2025-01-09 03:00:00')
),
fact_cons_hours_1 as (
   select *,
          lag(ts_hour) over(partition by user_id, site order by ts_hour) prev_ts_hour
     from pre_processed_fact_impressions
),
fact_cons_hours_2 as (
   select *,
          sum(case when prev_ts_hour != ts_hour - interval '1 hour' then 1 else 0 end)
            over (partition by user_id, site order by ts_hour) as grp
     from fact_cons_hours_1
    order by ts_hour
),
fact_cons_hours_3 as (
   select user_id, site,
          sum(num_of_events) as total_events,
          count(1) as num_of_consecutive_hours,
          grp
     from fact_cons_hours_2
    group by user_id, site, grp
)
select distinct f.user_id, f.site, r.apply_rule
  from fact_cons_hours_3       f
       inner join analyst_rules r
               on f.site = r.site
              and r.apply_rule = 'fraud'
 where (
          f.num_of_consecutive_hours >= r.hours_frame
          and
          f.total_events >= r.num_of_events
       )
;
