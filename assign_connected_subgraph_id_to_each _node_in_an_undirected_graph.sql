-- https://stackoverflow.com/questions/54658013/how-to-assign-connected-subgraph-id-to-each-node-in-an-undirected-graph-in-oracl
-- How to assign connected subgraph ID to each node in an undirected graph in Postgres SQL?
-- Theory is here https://en.wikipedia.org/wiki/Component_(graph_theory)


/*
   https://dbfiddle.uk/?rdbms=postgres_13&fiddle=67bec4951730f8975100961ec41b769f

   There is the following undirected graph:
   
               D
             /
           C            G
          /      E     /
         B            /
          \          F
           A
    
    Which is stored in tabular form with the following structure:
    
    create table edges(
       n1  text,  -- First vertex id
       n2  text   -- Second vertex id    
    );
    
    
    It is necessary to asssign connected component for each vertex of the graph:
    _________
    | n | c |
    ---------
    | A | 1 |
    | B | 1 |
    | C | 1 |
    | D | 1 |
    | E | 2 |
    | F | 3 |
    | G | 3 |
    ---------
    
*/

create table edges
as
select * from (
    values ('A', 'B'), ('B', 'C'), ('C', 'D'), ('E', null), ('F', 'G')
  ) as edges(n1, n2)
;

create table clusters
as
with recursive rec(an, n1, n2, np, cycle) as (
   select n.node an, p.n1, p.n2,
          array[row(p.n1, p.n2)] np,
          false
     from pairs p,
          nodes n
    where p.n1 = n.node
    union all
   select r.an, p.n1, p.n2,
          np||row(p.n1, p.n2),
          row(p.n1, p.n2) = any(np)
     from pairs p,
          rec   r
    where r.n2 = p.n1
      and not cycle
), pairs as (
   select n1, n2 from edges union select n2, n1 from edges
), nodes(node) as (
   select n1 from edges union select n2 from edges
), rec_res(an, node) as (
   select an, n1 from rec union select an, n2 from rec
)
select n.node n,
       dense_rank() over (order by array_agg(rr.node)) c
  from nodes   n,
       rec_res rr
 where n.node = rr.an
 group by n.node
 order by c, n
;
