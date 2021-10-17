-- https://docs.databricks.com/_static/notebooks/merge-in-scd-type-2.html
-- https://dbfiddle.uk/?rdbms=oracle_18&fiddle=9911c2e54a573069de7881625ed933ca

-- truncate table src_tbl;
-- truncate table trg_tbl;

-- DDL/DML src
create table src_tbl
(
    bk           varchar(100),  -- business_key
    val          varchar(20),
    num          number,
    start_date   date
)
;

insert all
  into src_tbl(bk, val, num, start_date) values ('key_1', 'c109', 99, trunc(sysdate)-10)
  into src_tbl(bk, val, num, start_date) values ('key_2', 'c209', 99, trunc(sysdate)-10)
  into src_tbl(bk, val, num, start_date) values ('key_3', 'c309', 99, trunc(sysdate)-10)
select * 
  from dual
;

-- DDL/DML trg
create table trg_tbl
(
    hk           raw(20) not null, -- hash_key
    val          varchar(20),
    num          number,
    start_date   date,
    end_date     date    not null,
    checksum     number,
    constraint trg_tbl_pk primary key (hk, end_date)
)
;

insert into trg_tbl(hk, val, num, start_date, end_date, checksum)
  with t(bk, val, num, sd, ed) as (
    select 'key_1', 'c101', 10, date'2018-01-01',  date'2018-02-01' from dual union all
    select 'key_1', 'c102', 20, date'2018-02-01',  date'9999-12-31' from dual union all
    select 'key_2', 'c309', 30, trunc(sysdate)-10, date'9999-12-31' from dual
  )
  select standard_hash('test'||bk), val, num, sd, ed, ora_hash(val||chr(1)||num)
    from t
;

-- show trg data
select RawToHex(hk), val, num,
       start_date, end_date
  from trg_tbl
 order by hk, start_date
;

-- Slowly Change Dimension Type 2 Merge (without tracking rows deleting on source)
merge /*+ enable_parallel_dml parallel(14) nologging */ into trg_tbl trg
   using (
     select /*+ parallel(14) */
            merge_key,
            standard_hash('test'||bk) hk,
            val, num, ora_hash(val||chr(1)||num) checksum
       from (
         select standard_hash('test'||bk) merge_key,
                bk, val, num
           from src_tbl
          union all
         select 
                null merge_key,
                s.bk, s.val, s.num
           from src_tbl            s
                inner join trg_tbl t
                        on standard_hash('test'||s.bk) = t.hk           
          where t.end_date = date'9999-12-31'
            and ora_hash(s.val||chr(1)||s.num) != t.checksum
       )
   ) src 
on (trg.hk = src.merge_key)
 when matched then
    update
       set trg.end_date = trunc(sysdate)
     where trg.end_date = date'9999-12-31'
       and trg.checksum   != src.checksum
 when not matched then
    insert(hk, val, num, start_date, end_date, checksum)
      values(src.hk, src.val, src.num, trunc(sysdate),
             date'9999-12-31', src.checksum)
;

-- show trg data after applying SCD 2
select RawToHex(hk), val, num,
       start_date, end_date
  from trg_tbl
 order by hk, start_date
;
