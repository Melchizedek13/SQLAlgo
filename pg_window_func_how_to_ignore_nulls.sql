-- https://stackoverflow.com/questions/18987791/how-do-i-efficiently-select-the-previous-non-null-value
-- https://dbfiddle.uk/jG8iLU-9

/*
There is a table in Postgres that looks like this:

 id | dt         | val
----+------------+---
| 1 | 2020-01-01 | a |
| 1 | 2020-02-01 |   |
| 1 | 2020-03-01 | b |
| 2 | 2021-01-01 | x |
| 2 | 2021-02-03 |   |

Needs to query it to make it look like this:

 id | dt         | val
----+------------+---
| 1 | 2020-01-01 | a |
| 1 | 2020-02-01 | a |
| 1 | 2020-03-01 | b |
| 2 | 2021-01-01 | x |
| 2 | 2021-02-03 | x |

*/

with
/*
   ds -> data set
   vp -> value partition
   rs -> running sum
   pnnv -> the previous non null vaule
*/
ds(id, dt, val) as (values
   (1, date'2020-01-01', 'a'),
   (1, date'2020-02-01', null),  -- a
   (1, date'2020-03-01', 'b'),
   (2, date'2021-01-01', 'x'),
   (2, date'2021-02-03', null)   -- x
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
