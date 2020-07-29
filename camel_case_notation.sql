-- Postgres


with t(id, txt) as (values
  (1, 'Lower_camel_cAse_notation_1'),
  (2, 'Lower_camel_case_notAtion_2')
)
select t.id,
       left(lower(t.txt), 1) ||
         substring(replace(initcap(t.txt), '_', ''), 2)
           as txt
  from t
;

with t(id, txt) as (values
  (1, 'upPer_camel_cAse_notation_1'),
  (2, 'uppEr_camel_case_notAtion_2')
)
select t.id, replace(initcap(t.txt), '_', '') txt
  from t
;

-- array
with t(id, txt) as (values
  (1, 'upper_camel_cAse_notation_1'),
  (2, 'upper_camel_case_notAtion_2')
)
select id, string_agg(txt, '')
  from (select id, initcap(unnest(string_to_array(txt, '_'))) txt from t) as r
 group by id
;

with t(id, txt) as (values
  (1, 'Lower_camel_cAse_notation_1'),
  (2, 'Lower_camel_case_notAtion_2')
)
select id, ARRAY_TO_STRING(
             lower(arr[1]) ||
             (select array_agg(initCap(n)) from unnest(arr[2:]) as n)
             , ''
           ) txt
  from (select id, string_to_array(txt, '_') arr from t ) as r
;

with t(id, txt) as (values
  (1, 'Lower_camel_cAse_notation_1'),
  (2, 'Lower_camel_case_notAtion_2')
)
select id, ARRAY_TO_STRING(
             lower(arr[1]) ||
             (select array_agg(initCap(n)) from unnest(arr[2:]) as n)
             , ''
           ) txt
  from (select id, string_to_array(txt, '_') arr from t ) as r
