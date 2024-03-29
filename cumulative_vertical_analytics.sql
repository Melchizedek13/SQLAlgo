/*
    Given the following table:
    _____________________
    |      d       |  b  |
    |______________|_____|
    | '2010-03-01' | 150 |
    | '2010-03-02' | 100 |
    | '2010-03-03' | 200 |
    | '2010-03-04' | 0   |
    | '2010-03-05' | 0   |
    | '2010-03-06' | 50  |
    | '2010-03-07' | 0   |
    | '2010-03-08' | 0   |
    | '2010-03-09' | 5   |
    | '2010-03-10' | 5   |
    | '2010-03-11' | 0   |
    |______________|_____|
    
    You need to get the following:
    ________________________________________
    | start_dt   | end_dt     | avg_balance|
    | 2010-03-01 | 2010-03-03 | 150        |
    | 2010-03-06 | 2010-03-06 | 50         |
    | 2010-03-09 | 2010-03-10 | 5          |
    |____________|____________| ___________|
*/

-- Postgresql
-- https://www.postgresql.org/docs/current/sql-expressions.html#SYNTAX-WINDOW-FUNCTIONS
--   frame_clause -> If frame_end is omitted, the end defaults to CURRENT ROW.
--     The default framing option is RANGE UNBOUNDED PRECEDING, which is the same as RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.

-- https://www.postgresql.org/docs/current/functions-window.html
--   When an aggregate function is used as a window function, it aggregates over the rows within the current row's window frame.
--     An aggregate used with ORDER BY and the default window frame definition produces a “running sum” type of behavior, which may or may not be what's wanted.
--       To obtain aggregation over the whole partition, omit ORDER BY or use ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING.
--     https://dbfiddle.uk/XcwxtSGI

-- https://dbfiddle.uk/wdeSa8Gh

with
t(d, b) as (values
  (date'2010-03-01', 150),
  (date'2010-03-02', 100),
  (date'2010-03-03', 200),
  (date'2010-03-04', 0),
  (date'2010-03-05', 0),
  (date'2010-03-06', 50),
  (date'2010-03-07', 0),
  (date'2010-03-08', 0),
  (date'2010-03-09', 5),
  (date'2010-03-10', 5),
  (date'2010-03-11', 0)
), extract_grp as (
   select *,
          case when coalesce(d - lag(d) over(order by d), 2) > 1
               then 1
               else 0
          end grp
     from t
    where b != 0
), trans_grp as (
   select d, b, sum(grp) over (order by d) grp_2
     from extract_grp
)
select min(d) start_dt, max(d) end_dt, avg(b)::int avg_balance
  from trans_grp
 group by grp_2
;
