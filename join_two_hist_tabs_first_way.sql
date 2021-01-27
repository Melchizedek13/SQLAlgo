/*
   Given two tables t1 and t2:

   ID|SD                 |ED                 |X|
   --|-------------------|-------------------|-|
    1|2018-01-01 00:00:00|2018-01-05 00:00:00|1|
    1|2018-01-05 00:00:01|5999-12-31 00:00:00|2|

   ID|SD                 |ED                 |Y|
   --|-------------------|-------------------|-|
    1|2018-01-01 00:00:00|2018-01-07 00:00:00|1|
    1|2018-01-07 00:00:01|5999-12-31 00:00:00|2|

   It is necessary to join two historical tables by the following way:

   ID|SD                 |ED                 |X|Y|
   --|-------------------|-------------------|-|-|
    1|2018-01-01 00:00:00|2018-01-05 00:00:00|1|1|
    1|2018-01-05 00:00:01|2018-01-07 00:00:00|2|1|
    1|2018-01-07 00:00:01|5999-12-30 23:59:59|2|2|
*/

with t1(id, sd, ed, x) as (values
   (1, timestamp'2018-01-01 00:00:00', timestamp'2018-01-05 00:00:00', 1),
   (1, timestamp'2018-01-05 00:00:01', timestamp'5999-12-31 00:00:00', 2)
), t2(id, sd, ed, y) as (values
   (1, timestamp'2018-01-01 00:00:00', timestamp'2018-01-07 00:00:00', 1),
   (1, timestamp'2018-01-07 00:00:01', timestamp'5999-12-31 00:00:00', 2)
), cuttPeriods as (
   select t1.id, t1.sd as p, t1.x as x, t2.y as y
     from t1 left join t2 on t1.id = t2.id and t1.sd between t2.sd and t2.ed
    union
   select t2.id, t2.sd as p, t1.x as x, t2.y as y
     from t2 left join t1 on t2.id = t1.id and t2.sd between t1.sd and t1.ed
)
select id,
       p as sd,
       lead(p, 1, timestamp'5999-12-31 00:00:00') over (partition by id order by p) - interval '1 second',
       x, y
  from cuttPeriods
 order by id, p;
;
