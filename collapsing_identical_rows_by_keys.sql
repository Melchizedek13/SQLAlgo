/* Description of the task
   The key is given, consisting of three attributes: id, c1, c2.
   It is necessary to collapse rows with the same key values,
     keeping the technical attribute (dml_type, load_dt) values in the result set.
   
   Input data set:
    |ID|C1|C2|C3|DML_TYPE|LOAD_DT   |
    |--|--|--|--|--------|----------|
    | 1|A1|B1|C1|I       |2020-01-01|
    | 1|A1|B1|C3|U       |2020-01-03|
    | 1|A1|B2|C3|U       |2020-01-04|
    | 1|A1|B1|C2|U       |2020-01-02|
    | 1|A1|B1|C3|U       |2020-01-05|
    
   Algorithm result:
    |ID|C1|C2|DML_TYPE|LOAD_DT   |
    |--|--|--|--------|----------|
    | 1|A1|B1|I       |2020-01-01|
    | 1|A1|B2|U       |2020-01-04|
    | 1|A1|B1|U       |2020-01-05|
*/

-- Postgres
with t(id, c1, c2, c3, dml_type, load_dt) as (values
     (1, 'A1', 'B1', 'C1', 'I', date'2020-01-01'),
     (1, 'A1', 'B1', 'C2', 'U', date'2020-01-02'), -- Collapse
     (1, 'A1', 'B1', 'C3', 'U', date'2020-01-03'), -- Collapse
     (1, 'A1', 'B2', 'C3', 'U', date'2020-01-04'),
     (1, 'A1', 'B1', 'C3', 'U', date'2020-01-05')
), agg as (
    select id, c1, c2, dml_type, load_dt,
           case when array[row(id, c1, c2)] = lag(array[row(id, c1, c2)])
                         over (partition by id order by load_dt)
                then 0
                else 1
           end StartFlag
      from t
)
select *
  from agg
 where startflag = 1
 order by id, load_dt
;

-- Exasol
with t(id, c1, c2, c3, dml_type, load_dt) as (values
     (1, 'A1', 'B1', 'C1', 'I', date'2020-01-01'),
     (1, 'A1', 'B1', 'C2', 'U', date'2020-01-02'), -- Collapse
     (1, 'A1', 'B1', 'C3', 'U', date'2020-01-03'), -- Collapse
     (1, 'A1', 'B2', 'C3', 'U', date'2020-01-04'),
     (1, 'A1', 'B1', 'C3', 'U', date'2020-01-05')
), agg as (
    select id, c1, c2, dml_type, load_dt,
           -- lag(0, 1, 1) over (partition by id, c1, c2 order by load_dt) key_was_changed,
           case when hash_md5(id, c1, c2) = lag(hash_md5(id, c1, c2)) over (partition by id order by load_dt)
                then 0
                else 1
           end StartFlag
      from t
)
select *
  from agg
 where startflag = 1
 order by id, load_dt
;

/*
   Same as above. But taking into account blank(empty) lines and NULL values.
*/

-- Postgres
with t(id, c1, c2, c3, dml_type, load_dt) as (values
     (1, '',   null, 'A1', 'I', timestamp'2019-12-28 00:00:00'),
     (1, null, '',   'B1', 'U', timestamp'2019-12-29 00:00:00'), -- Collapse
     (1, 'A1', null, 'B2', 'U', timestamp'2019-12-30 00:00:00'),
     (1, null, 'B3', 'B3', 'U', timestamp'2019-12-31 00:00:00'),
     (1, 'A1', 'B1', 'C1', 'U', timestamp'2020-01-01 00:00:00'),
     (1, 'A1', 'B1', 'C2', 'U', timestamp'2020-01-02 00:00:00'), -- Collapse
     (1, 'A1', 'B1', 'C3', 'U', timestamp'2020-01-03 00:00:00'), -- Collapse
     (1, 'A1', 'B2', 'C3', 'U', timestamp'2020-01-04 00:00:00'),
     (1, 'A1', 'B1', 'C3', 'U', timestamp'2020-01-05 00:00:00'),
     (2, '',   null, 'A1', 'I', timestamp'2019-12-28 12:00:00'),
     (2, null, '',   'B1', 'U', timestamp'2019-12-29 12:00:00'), -- Collapse
     (2, 'A1', null, 'B2', 'U', timestamp'2019-12-30 12:00:00'),
     (2, null, 'B3', 'B3', 'U', timestamp'2019-12-31 00:00:00'),
     (2, 'A1', 'B1', 'C1', 'U', timestamp'2020-01-01 00:00:00'),
     (2, 'A1', 'B1', 'C2', 'U', timestamp'2020-01-02 00:00:00'), -- Collapse
     (2, 'A1', 'B1', 'C3', 'U', timestamp'2020-01-03 00:00:00'), -- Collapse
     (2, 'A1', 'B2', 'C3', 'U', timestamp'2020-01-04 00:00:00'),
     (2, 'A1', 'B1', 'C3', 'U', timestamp'2020-01-05 00:00:00')
), agg as (
    select id, c1, c2, dml_type, load_dt,
           case when array[row(id, nullif(c1, ''), nullif(c2, ''))]
                     = lag(array[row(id, nullif(c1, ''), nullif(c2, ''))])
                         over (partition by id order by load_dt)
                then False
                else True
           end StartFlag
      from t
)
select *
  from agg
 where startflag
 order by id, load_dt
;

-- Exasol
with t(id, c1, c2, c3, dml_type, load_dt) as (values
     (1, '',   null, 'A1', 'I', timestamp'2019-12-28 00:00:00'),
     (1, null, '',   'B1', 'U', timestamp'2019-12-29 00:00:00'), -- Collapse
     (1, 'A1', null, 'B2', 'U', timestamp'2019-12-30 00:00:00'),
     (1, null, 'B3', 'B3', 'U', timestamp'2019-12-31 00:00:00'),
     (1, 'A1', 'B1', 'C1', 'U', timestamp'2020-01-01 00:00:00'),
     (1, 'A1', 'B1', 'C2', 'U', timestamp'2020-01-02 00:00:00'), -- Collapse
     (1, 'A1', 'B1', 'C3', 'U', timestamp'2020-01-03 00:00:00'), -- Collapse
     (1, 'A1', 'B2', 'C3', 'U', timestamp'2020-01-04 00:00:00'),
     (1, 'A1', 'B1', 'C3', 'U', timestamp'2020-01-05 00:00:00'),
     (2, '',   null, 'A1', 'I', timestamp'2019-12-28 12:00:00'),
     (2, null, '',   'B1', 'U', timestamp'2019-12-29 12:00:00'), -- Collapse
     (2, 'A1', null, 'B2', 'U', timestamp'2019-12-30 12:00:00'),
     (2, null, 'B3', 'B3', 'U', timestamp'2019-12-31 00:00:00'),
     (2, 'A1', 'B1', 'C1', 'U', timestamp'2020-01-01 00:00:00'),
     (2, 'A1', 'B1', 'C2', 'U', timestamp'2020-01-02 00:00:00'), -- Collapse
     (2, 'A1', 'B1', 'C3', 'U', timestamp'2020-01-03 00:00:00'), -- Collapse
     (2, 'A1', 'B2', 'C3', 'U', timestamp'2020-01-04 00:00:00'),
     (2, 'A1', 'B1', 'C3', 'U', timestamp'2020-01-05 00:00:00')
), agg as (
    select t.*,
           decode(
              hash_md5(id, c1, c2),
              lag(hash_md5(id, c1, c2)) over (partition by id order by load_dt),
              False,
              True
           ) as keep_row_flg
      from t
)
select *
  from agg
 where keep_row_flg
 order by id, load_dt
;
