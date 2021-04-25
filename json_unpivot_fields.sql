-- SparkSQL (Outer)
select student_id,
       explode_outer(from_json(js, 'map<string, timestamp>')) as (subject, date_till)
  from values
          (1, '{"1": "2022-03-24T18:36:25.576048", "7": "2022-03-24T18:36:25.565574", "math": "2022-03-24T18:36:25.580098", "coding": "2022-03-24T18:36:25.570845", "laboratory": "2022-03-24T18:36:25.570845"}'),
          (2, '{}')
       as data(student_id, js)
;

-- Postgres (Outer)
with t(id, subscriptions) as (values
  ('0cf2d3e9-3b4c-4308-8b74-9c6d6255e40f', '{"1": "2022-03-24T18:36:25.576048", "7": "2022-03-24T18:36:25.565574", "math": "2022-03-24T18:36:25.580098", "coding": "2022-03-24T18:36:25.570845", "laboratory": "2022-03-24T18:36:25.570845"}'),
  ('d518b53e-cd3e-410b-b000-26df1e67aaae', '{}')
)
select t.id::uuid, js.key as subject,
       to_timestamp(js.value, 'YYYY-MM-DD"T"HH24:MI:SS') as date_till
  from t
       left join jsonb_each_text(t.subscriptions::jsonb) as js
              on true
;

-- ClickHouse (Outer)
select id,
       kv.1 as subject,
       parseDateTimeBestEffortOrNull(nullIf(trim(both '"' from kv.2), '')) as date_till
  from values ('id UUID, subscriptions String',
               ('0cf2d3e9-3b4c-4308-8b74-9c6d6255e40f', '{"1": "2022-03-24T18:36:25.576048", "7": "2022-03-24T18:36:25.565574", "math": "2022-03-24T18:36:25.580098", "coding": "2022-03-24T18:36:25.570845", "laboratory": "2022-03-24T18:36:25.570845"}'),
               ('d518b53e-cd3e-410b-b000-26df1e67aaae', '{}')
  )
       left array join JSONExtractKeysAndValuesRaw(subscriptions) as kv
;

-- ClickHouse (Inner)
  with arrayJoin(JSONExtractKeysAndValuesRaw(subscriptions)) as kv
select id,
       kv.1 as subject,
       parseDateTimeBestEffort(trim(both '"' from kv.2)) as date_till
  from values ('id UUID, subscriptions String',
               ('0cf2d3e9-3b4c-4308-8b74-9c6d6255e40f', '{"1": "2022-03-24T18:36:25.576048", "7": "2022-03-24T18:36:25.565574", "math": "2022-03-24T18:36:25.580098", "coding": "2022-03-24T18:36:25.570845", "laboratory": "2022-03-24T18:36:25.570845"}'),
               ('d518b53e-cd3e-410b-b000-26df1e67aaae', '{}')
  )
;
