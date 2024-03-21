/*
    There is a table in DBMS that looks like this:
    
    _________________________
    | id | dt         | val |
    |----+------------+-----|
    | 1  | 2020-01-01 | a   |
    | 1  | 2020-02-01 |     |
    | 1  | 2020-03-01 | b   |
    | 2  | 2021-01-01 | x   |
    | 2  | 2021-02-03 |     |
    |____|____________|_____|
    
    Needs to query it to make it look like this:
    
    ________________________
    | id | dt         | val |
    |----+------------+-----|
    | 1  | 2020-01-01 | a   |
    | 1  | 2020-02-01 | a   |
    | 1  | 2020-03-01 | b   |
    | 2  | 2021-01-01 | x   |
    | 2  | 2021-02-03 | x   |
    |____|____________|_____|

*/

-- Postgresql
-- https://stackoverflow.com/questions/18987791/how-do-i-efficiently-select-the-previous-non-null-value
-- https://dbfiddle.uk/jG8iLU-9

-- https://www.postgresql.org/docs/current/sql-expressions.html#SYNTAX-WINDOW-FUNCTIONS
--   frame_clause -> If frame_end is omitted, the end defaults to CURRENT ROW.
--     The default framing option is RANGE UNBOUNDED PRECEDING, which is the same as RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.

-- https://www.postgresql.org/docs/current/functions-window.html
--   Note that first_value, last_value, and nth_value consider only the rows within the “window frame”,
--     which by default contains the rows from the start of the partition through the last peer of the current row.
--
--   When an aggregate function is used as a window function, it aggregates over the rows within the current row's window frame.
--     An aggregate used with ORDER BY and the default window frame definition produces a “running sum” type of behavior, which may or may not be what's wanted.
--       To obtain aggregation over the whole partition, omit ORDER BY or use ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING.
--         Other frame specifications can be used to obtain other effects.

with
/*
   ds -> data set
   vp -> value partition
   rs -> running sum
   pnnv -> the previous non null vaule
*/
ds(id, dt, val) as (values
   (1, date'2020-01-01', 'a'),
   (1, date'2020-02-01', null),
   (1, date'2020-03-01', 'b'),
   (2, date'2021-01-01', 'x'),
   (2, date'2021-02-03', null)
),
rs as (
   select id, dt, val,
          sum(case when val is null then 0 else 1 end)
             over (partition by id order by dt)
                as vp
     from ds
)
select id, dt, val, vp,
       first_value(val) over (partition by id, vp)
          as val_pnnv
  from rs
 order by id, dt
;


-- Oracle
-- https://oracle-base.com/articles/misc/first-value-and-last-value-analytic-functions#last-value
--   The `last_value(...)` default windowing clause is "range between unbounded preceding and current row"
-- https://dbfiddle.uk/gX09QA5B
with
ds(id, dt, val) as (values
   (1, date'2020-01-01', 'a'),
   (1, date'2020-02-01', null),
   (1, date'2020-03-01', 'b'),
   (2, date'2021-01-01', 'x'),
   (2, date'2021-02-03', null)
)
select id, dt, val,
       last_value(val) ignore nulls
          over (partition by id order by dt)
             as val_pnnv
  from ds
 order by id, dt
;

-- Snowflake
/* https://docs.snowflake.com/en/sql-reference/functions/last_value
   If no window_frame is specified, the default is the entire window:
      - rows between unbounded preceding and unbounded following

   This differs from the ANSI standard, which specifies the following default for window frames:
      - range between unbounded preceding and current row
*/
