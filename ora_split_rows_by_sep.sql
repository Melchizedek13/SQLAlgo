/* It is necessary to split the strings into substrings by separator, keeping the string ID. */

with t as (
   select 1 id, 'Attention, here we present a standard approach in oracle, which split strings by a separator.' c2 from dual
    union all
   select 2, 'abc, def, gek' from dual
)
select id,
       ltrim(regexp_substr(c2, '[^,]+', 1, level)) as str
  from t
connect by regexp_substr(c2, '[^,]+', 1, level) is not null
    and prior id = id
    and not prior sys_guid() is null -- prior dbms_random.value != 1
;
