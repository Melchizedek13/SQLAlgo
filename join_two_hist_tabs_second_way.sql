/*
   Given two tables t1 and t2:

   ID|SD                 |ED                 |X|
   --|-------------------|-------------------|-|
    1|2018-01-01 00:00:00|2018-01-03 00:00:00|1|
    1|2018-01-03 00:00:01|2018-01-05 00:00:00|2|
    1|2018-01-05 00:00:01|5999-12-31 00:00:00|3|

   ID|SD                 |ED                 |Y|
   --|-------------------|-------------------|-|
    1|2018-01-01 00:00:00|2018-01-03 00:00:00|4|
    1|2018-01-05 00:00:01|5999-12-31 00:00:00|5|

   It is necessary to join two historical tables by the following way:

   ID|SD                 |ED                 |X|Y|
   --|-------------------|-------------------|-|-|
    1|2018-01-01 00:00:00|2018-01-03 00:00:00|1|4|
    1|2018-01-03 00:00:01|2018-01-05 00:00:00|2| |
    1|2018-01-05 00:00:01|5999-12-31 00:00:00|3|5|
*/

with t1(id, sd, ed, x) as (values
   (1, timestamp'2018-01-01 00:00:00', timestamp'2018-01-03 00:00:00', 1),
   (1, timestamp'2018-01-03 00:00:01', timestamp'2018-01-05 00:00:00', 2),
   (1, timestamp'2018-01-05 00:00:01', timestamp'5999-12-31 00:00:00', 3)
), t2(id, sd, ed, y) as (values
   (1, timestamp'2018-01-01 00:00:00', timestamp'2018-01-03 00:00:00', 4),
   (1, timestamp'2018-01-05 00:00:01', timestamp'5999-12-31 00:00:00', 5)
), all_points(id, point) as (
   select id, sd from t1
    union
   select id, ed + interval '1 second' as point from t1 where ed != timestamp'5999-12-31 00:00:00'
    union
   select id, sd from t2
    union
   select id, ed + interval '1 second' as point from t2 where ed != timestamp'5999-12-31 00:00:00'
), collapsing_points(id, p, ed) as (
   select distinct id, point,
          lead(point - interval '1 second') over (partition by id order by point) as ed
     from all_points
)
select rs.id,
       rs.p as sd,
       coalesce(rs.ed, timestamp'5999-12-31 00:00:00') as ed,
       t1.x, t2.y
  from collapsing_points rs
       left join t1 on rs.id = t1.id and rs.p between t1.sd and t1.ed
       left join t2 on rs.id = t2.id and rs.p between t2.sd and t2.ed
 order by 1, 2, 3
;
