/* https://habr.com/ru/companies/sberbank/articles/519358/ - Combining multiple stories into one
   
   When loading data into data warehouses (DWHs), the problem is often solved when you need to build a single history for an entity,
     having separate histories of the attributes of this entity that came from different sources.

   Algo
   First, a diagonal table `dt` is formed with data from different sources for different attributes (attributes missing from the source are filled with nulls).
   Then, using the last_value(* ignore nulls) function, the diagonal table collapses into a single history `ciss`, and the last known attribute values are extended
      forward to those dates on which there were no changes for them.
*/

-- Oracle
-- https://dbfiddle.uk/e4RMHisY
-- alter session set nls_date_format='yyyy-mm-dd';

with
/*
   dt -> diagonal table
   ciss -> collapses into a single story
*/
hist1(id, load_dt, attr1) as (values
   (1, date'2014-01-01', 7),
   (1, date'2015-01-01', 8),
   (1, date'2016-01-01', 9),
   (2, date'2014-01-01', 17),
   (2, date'2015-01-01', 18),
   (2, date'2016-01-01', 19)
),
hist2(id, load_dt, attr2) as (values
   (1, date'2015-01-01', 4),
   (1, date'2016-01-01', 5),
   (1, date'2017-01-01', 6),
   (2, date'2015-01-01', 14),
   (2, date'2016-01-01', 15),
   (2, date'2017-01-01', 16)
),
hist3(id, load_dt, attr3) as (values
   (1, date'2016-01-01', 10),
   (1, date'2017-01-01', 20),
   (1, date'2018-01-01', 30),
   (2, date'2016-01-01', 110),
   (2, date'2017-01-01', 120),
   (2, date'2018-01-01', 130)
),
dt as (
   select id,
          load_dt,
          attr1,
          cast(null as number) attr2,
          cast(null as number) attr3
     from hist1
    union all
   select id,
          load_dt,
          cast(null as number) attr1,
          attr2,
          cast(null as number) attr3
     from hist2
    union all
   select id,
          load_dt,
          cast(null as number) attr1,
          cast(null as number) attr2,
          attr3
     from hist3
),
ciss as (
   select id,
          load_dt,
          max(attr1) as attr1,
          max(attr2) as attr2,
          max(attr3) as attr3
     from dt
    group by id, load_dt
)
select id,
       load_dt as start_dt,
       nvl(
          lead(load_dt - 1) over(partition by id order by load_dt),
          date'9999-12-31'
       ) as end_dt,
       last_value(attr1 ignore nulls) over(partition by id order by load_dt) as attr1,
       last_value(attr2 ignore nulls) over(partition by id order by load_dt) as attr2,
       last_value(attr3 ignore nulls) over(partition by id order by load_dt) as attr3
  from ciss
 order by id, load_dt
;
