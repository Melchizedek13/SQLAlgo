/*
https://leetcode.com/problems/consecutive-numbers/

Find all numbers that appear at least three times consecutively. Return the result table in any order.

The result format is in the following example.

Example 1:

Input: 
Logs table:
+----+-----+
| id | num |
+----+-----+
| 1  | 1   |
| 2  | 1   |
| 3  | 1   |
| 4  | 2   |
| 5  | 1   |
| 6  | 2   |
| 7  | 2   |
+----+-----+

Output:
+-----------------+
| ConsecutiveNums |
+-----------------+
| 1               |
+-----------------+

Explanation: 1 is the only number that appears consecutively for at least three times.

Table: Logs
+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| id          | int     |
| num         | varchar |
+-------------+---------+

In SQL, id is the primary key for this table. id is an autoincrement column.
*/

-- MySQL. 1
with
t_grp as (
   select num,
          id - cast(row_number() over(partition by num order by id) as signed) grp_id
     from Logs
),
p as (
   select num as ConsecutiveNums
     from t_grp
    group by num, grp_id
   having count(*) >= 3
)
select distinct ConsecutiveNums
  from p
;

-- MySQL. 2
with
cte as (
   select num,
          lead(num,1) over() num1,
          lead(num,2) over() num2
     from logs
)
select distinct num ConsecutiveNums
  from cte
where (num=num1) and (num=num2)
;

-- MySQL. 3
select distinct a.num as ConsecutiveNums
  from Logs a
       inner join Logs b on b.id=a.id+1 and b.num=a.num
       inner join Logs c on c.id=a.id+2 and c.num=a.num
;
