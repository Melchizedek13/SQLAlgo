-- Postgres way with analytics
with item_sales(item_id, price, dt) as (
	values(1, 100, date'2020-03-01'),
	      (1, 300, date'2020-03-04'),
	      (1, 200, date'2020-03-07'),
	      (2, 150, date'2020-03-06')
)
select item_id, price, dt
  from (
    select *, row_number() over (partition by item_id order by dt desc) rd
      from item_sales
  ) rs
where rs.rd = 1;
