show databases ;
--回顾所有的窗口函数
--创建一个数据集
create or replace temporary view mytab(cid,name,score) as values
(1,'zs',60),
(1,'ls',70),
(1,'qq',70),
(1,'ww',80),
(1,'zl',90),
(2,'aa',80),
(2,'bb',90);

select * from mytab;
--设置shuffle的并行度
set spark.sql.shuffle.partitions=4;
--排序
select *,
       row_number() over (partition by cid order by score) rn,
       rank() over (partition by cid order by score) rk,
       dense_rank() over (partition by cid order by score) drk,
--聚合
       sum(score) over (partition by cid) as sum1,
       sum(score) over (partition by cid order by score) sum2,
       --等价于
       sum(score) over (partition by cid order by score range between unbounded preceding and current row ) sum3,
       sum(score) over (partition by cid order by score rows between unbounded preceding and current row ) sum4,
       sum(score) over (partition by cid order by score desc) as sum5,
       --等价于
       sum(score) over (partition by cid order by score range between current row and unbounded following) as sum6,
       --等价于
       sum(score) over (partition by cid order by score desc range between unbounded preceding and current row) as sum7,
--向前向后取数
       lag(score,1,0) over (partition by cid order by score) lag1,
       lag(score) over (partition by cid order by score ) lag2,
       lead(score) over (partition by cid order by score) lead1,
--first_value,last_value
       first_value(score) over (partition by cid order by score) first,
       last_value(score) over (partition by cid order by score rows between unbounded preceding and unbounded following) last
from mytab;

