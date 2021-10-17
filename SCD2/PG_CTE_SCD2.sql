-- https://dannybeachnau.com/2020/02/05/creating-a-slowly-changing-dimension-in-postgres/
-- https://dbfiddle.uk/?rdbms=postgres_13&fiddle=d48a1515d0a1f4d635e93a8507df66c7 - 1 way
-- https://dbfiddle.uk/?rdbms=postgres_13&fiddle=94e172f26bf5c1e57f3d5f504cfa358a - 2 way

-- DDL. Trg 
create table dim_customer (
    customer_id    int,
    name           varchar(32),
    phone_number   character(12),
    valid_from     timestamp(0) not null default now() - interval '5' second,
    valid_to       timestamp(0) default timestamp'9999-12-31 23:59:59',
    surrogate_key  serial primary key
)
;

-- DML. Trg
insert into dim_customer (customer_id, name, phone_number)
values
    (1, 'Dave', '415-381-0912'),
    (2, 'Joe',  '287-230-1234'),
    (3, 'Sue',  '772-112-0391')
;

-- SCD2. 1 way
with
-- Contains the data to be inserted
inserts (customer_id, name, phone_number, valid_from) as (
    select *, now() from (values
        (1, 'Dave'::varchar(32), '415-381-0912'::character(12)), -- changed
        (2, 'Joe'::varchar(32),  '222-222-2222'::character(12)), -- exists
        (4, 'Leo'::varchar(32),  '333-333-333'::character(12))   -- new
    ) t
)
,
-- Updates the old rows
updates as (
    update dim_customer t
       set valid_to = now() - interval '1' second
      from inserts s
     where t.customer_id = s.customer_id
       and t.valid_to = timestamp'9999-12-31 23:59:59'
       and array[row(t.name, t.phone_number)]
           !=
           array[row(s.name, s.phone_number)]
)
-- Insert the new data
insert into dim_customer
  select s.*
   from inserts                s
        left join dim_customer t
               on s.customer_id = t.customer_id
              and t.valid_to = timestamp'9999-12-31 23:59:59'
  where t.customer_id is null
     or array[row(t.name, t.phone_number)]
        !=
        array[row(s.name, s.phone_number)]
;

-- SCD2. 2 way
with
-- Contains the data to be inserted
inserts (customer_id, name, phone_number, valid_from) as (
    select *, now() from (values
        (1, 'Dave'::varchar(32), '415-381-0912'::character(12)), -- changed
        (2, 'Joe'::varchar(32),  '222-222-2222'::character(12)), -- exists
        (4, 'Leo'::varchar(32),  '333-333-333'::character(12))   -- new
    ) t
)
,
-- Updates the old rows and returns a list of the updated IDs
updates (customer_id) as (
    update dim_customer t
       set valid_to = now() - interval '1' second
      from inserts s
     where t.customer_id = s.customer_id
       and t.valid_to = timestamp'9999-12-31 23:59:59'
       and array[row(t.name, t.phone_number)]
           !=
           array[row(s.name, s.phone_number)]
 returning s.customer_id
)
-- Insert the new data
insert into dim_customer
  select s.*
   from inserts                s
        left join dim_customer t
               on s.customer_id = t.customer_id
  where t.customer_id is null
     or exists (
             select 1
               from updates u
              where u.customer_id = s.customer_id
           )
;

-- Show data
select *
  from dim_customer
 order by customer_id, valid_from, surrogate_key
;
