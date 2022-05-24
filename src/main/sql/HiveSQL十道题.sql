create database test_sql;
use test_sql;
--第一题
CREATE TABLE test_sql.test1 (
                                userId string,
                                visitDate string,
                                visitCount INT )
    ROW format delimited FIELDS TERMINATED BY "\t";
INSERT INTO TABLE test_sql.test1
VALUES
( 'u01', '2017/1/21', 5 ),
( 'u02', '2017/1/23', 6 ),
( 'u03', '2017/1/22', 8 ),
( 'u04', '2017/1/20', 3 ),
( 'u01', '2017/1/23', 6 ),
( 'u01', '2017/2/21', 8 ),
( 'u02', '2017/1/23', 6 ),
( 'u01', '2017/2/22', 4 );
set spark.sql.shuffle.partitions=4;
select *,
       sum(sum1) over (partition by userId order by visitMonth rows between unbounded preceding and current row ) as sum2--累积
from
(select userId,
       substr(visitDate,0,6) as visitMonth,
       sum(visitCount) as sum1 --小计
from test1
group by userId,substr(visitDate,0,6)) t
order by userId,visitMonth;
--优化1
select *,
       sum(sum1) over (partition by userId order by visitMonth rows between unbounded preceding and current row ) as sum2--累积
from
(select userId,
        date_format(regexp_replace(visitDate,'/','-'),'yyyy-MM')  as visitMonth,
       sum(visitCount) as sum1 --小计
from test1
group by userId,date_format(regexp_replace(visitDate,'/','-'),'yyyy-MM') ) t
order by userId,visitMonth;


--第二题
CREATE TABLE test_sql.test2 (
                                user_id string,
                                shop string )
    ROW format delimited FIELDS TERMINATED BY '\t';
INSERT INTO TABLE test_sql.test2 VALUES
( 'u1', 'a' ),
( 'u2', 'b' ),
( 'u1', 'b' ),
( 'u1', 'a' ),
( 'u3', 'c' ),
( 'u4', 'b' ),
( 'u1', 'a' ),
( 'u2', 'c' ),
( 'u5', 'b' ),
( 'u4', 'b' ),
( 'u6', 'c' ),
( 'u2', 'c' ),
( 'u1', 'b' ),
( 'u2', 'a' ),
( 'u2', 'a' ),
( 'u3', 'a' ),
( 'u5', 'a' ),
( 'u5', 'a' ),
( 'u5', 'a' );

select * from test2 order by shop,user_id;
--请统计：
-- （1）每个店铺的UV（访客数）
--uv --user visit
--pv --page visit
--方式1
select shop,
       count(distinct user_id) as uv
from test2 group by shop;
--方式2
select shop,
       count(user_id) as uv
 from
(select user_id,shop
from test2 group by user_id,shop) t
group by shop ;



-- (2)每个店铺访问次数top3的访客信息。输出店铺名称、访客id、访问次数
--语法1
select *
from
(select shop,
       user_id,
       cnt,
       row_number() over (partition by shop order by cnt desc) rn
from
(select user_id,shop,
       count(*) as cnt
from test2
group by user_id,shop
order by shop,user_id) t)
where rn<=3;

--语法2 用with as
with t1 as (
    select user_id,
           shop,
           count(*) as cnt
    from test2
    group by user_id, shop
    order by shop, user_id
),
     t2 as (
         select shop,
                user_id,
                cnt,
                row_number() over (partition by shop order by cnt desc) rn
         from t1
     )
select *from t2 where rn<=3 order by shop ,cnt desc;

--语法3 hive不支持临时视图
create or replace temporary view view1 as
select user_id,
       shop,
       count(*) as cnt
from test2
group by user_id, shop
order by shop, user_id;
create or replace temporary view view2 as
select shop,
        user_id,
        cnt,
        row_number() over (partition by shop order by cnt desc) rn
 from view1;
select *from view2 where rn<=3 order by shop ,cnt desc;


CREATE TABLE test_sql.test3 (
                                dt string,
                                order_id string,
                                user_id string,
                                amount DECIMAL ( 10, 2 ) )
    ROW format delimited FIELDS TERMINATED BY '\t';
INSERT INTO TABLE test_sql.test3 VALUES ('2017-01-01','10029028','1000003251',33.57);
INSERT INTO TABLE test_sql.test3 VALUES ('2017-01-01','10029029','1000003251',33.57);
INSERT INTO TABLE test_sql.test3 VALUES ('2017-01-01','100290288','1000003252',33.57);
INSERT INTO TABLE test_sql.test3 VALUES ('2017-02-02','10029088','1000003251',33.57);
INSERT INTO TABLE test_sql.test3 VALUES ('2017-02-02','10028888','1000008888',33.57);
INSERT INTO TABLE test_sql.test3 VALUES ('2017-02-02','100290281','1000003251',33.57);
INSERT INTO TABLE test_sql.test3 VALUES ('2017-02-02','100290282','1000003253',33.57);
INSERT INTO TABLE test_sql.test3 VALUES ('2017-11-02','10290282','100003253',234);
INSERT INTO TABLE test_sql.test3 VALUES ('2018-11-02','10290284','100003243',234);

select * from test3;
--(1)给出 2017年每个月的订单数、用户数、总成交金额。
select date_format(dt, 'yyyy-MM') as month1,
       count(order_id)            as cnt_orders,--订单数
       count(distinct user_id)    as cnt_users,--用户数
       sum(amount)                as sum_amt--总成交金额
from test3 where year(dt)=2017
group by date_format(dt, 'yyyy-MM');
--语法2
select date_format(dt, 'yyyy-MM') as month1,
       count(order_id)            as cnt_orders,--订单数
       count(distinct user_id)    as cnt_users,--用户数
       sum(amount)                as sum_amt--总成交金额
from test3 where date_format(dt,'yyyy')=2017
group by date_format(dt, 'yyyy-MM');

-- (2)给出2017年2月的新客数(指在2月才有第一笔订单)
with t1 as (
    select *,
       date_format(dt,'yyyy-MM') as ym
from test3
),
     t2 as (
         select user_id,
              min(ym) as min_ym
         from t1
         group by user_id
     )
select count(user_id) as cnt from t2 where min_ym='2017-02';


--第四题
CREATE TABLE test_sql.test4user
(
    user_id string,
    name    string,
    age     int
);
CREATE TABLE test_sql.test4log
(
    user_id string,
    url     string
);
INSERT INTO TABLE test_sql.test4user
VALUES ('001', 'u1', 10);
INSERT INTO TABLE test_sql.test4user
VALUES ('002', 'u2', 15);
INSERT INTO TABLE test_sql.test4user
VALUES ('003', 'u3', 15);
INSERT INTO TABLE test_sql.test4user
VALUES ('004', 'u4', 20);
INSERT INTO TABLE test_sql.test4user
VALUES ('005', 'u5', 25);
INSERT INTO TABLE test_sql.test4user
VALUES ('006', 'u6', 35);
INSERT INTO TABLE test_sql.test4user
VALUES ('007', 'u7', 40);
INSERT INTO TABLE test_sql.test4user
VALUES ('008', 'u8', 45);
INSERT INTO TABLE test_sql.test4user
VALUES ('009', 'u9', 50);
INSERT INTO TABLE test_sql.test4user
VALUES ('0010', 'u10', 65);
INSERT INTO TABLE test_sql.test4log
VALUES ('001', 'url1');
INSERT INTO TABLE test_sql.test4log
VALUES ('002', 'url1');
INSERT INTO TABLE test_sql.test4log
VALUES ('003', 'url2');
INSERT INTO TABLE test_sql.test4log
VALUES ('004', 'url3');
INSERT INTO TABLE test_sql.test4log
VALUES ('005', 'url3');
INSERT INTO TABLE test_sql.test4log
VALUES ('006', 'url1');
INSERT INTO TABLE test_sql.test4log
VALUES ('007', 'url5');
INSERT INTO TABLE test_sql.test4log
VALUES ('008', 'url7');
INSERT INTO TABLE test_sql.test4log
VALUES ('009', 'url5');
INSERT INTO TABLE test_sql.test4log
VALUES ('0010', 'url1');
--有一个5000万的用户文件(user_id，name，age)，一个2亿记录的用户看电影的记录文件(user_id， url)，根据年龄段观看电影的次数进行排序？
with t1 as (
    select user_id,
           count(url) as cnt
    from test4log
    group by user_id
),
     t2 as (
         select user_id,
                age,
                case when age>=0 and age<=10 then '0-10'
                     when age>10 and age<=20 then '10-20'
                     when age>20 and age<=30 then '20-30'
                     when age>30 and age<=40 then '30-40'
                     when age>40 and age<=50 then '40-50'
                     when age>50 and age<=60 then '50-60'
                     when age>60 and age<=70 then '60-70'
                end age_phase
        from test4user
     ),
     t3 as (
         select age_phase,
                sum(cnt) sum1
         from t1 join t2 on t1.user_id=t2.user_id
         group by t2.age_phase
     )
select * from t3;

--方案二
with t1 as (
    select user_id,
           count(url) as cnt
    from test4log
    group by user_id
),
     t2 as (
         select user_id,
                age,
                concat( floor(age/10)*10,'-',(floor(age/10)+1)*10) as age_phase
        from test4user
     ),
     t3 as (
         select age_phase,
                sum(cnt) sum1
         from t1 join t2 on t1.user_id=t2.user_id
         group by t2.age_phase
     )
select * from t3;

select floor(15/10)*10 as x;
select ceil(15/10)*10 as x;
select concat( floor(15/10)*10,'-',ceil(15/10)*10) as x;
select concat( floor(25/10)*10,'-',ceil(25/10)*10) as x;
select concat( floor(20/10)*10,'-',(floor(20/10)+1)*10) as x;

--第五题
CREATE TABLE test5
(
    dt      string,
    user_id string,
    age     int
) ROW format delimited fields terminated BY ',';
INSERT INTO TABLE test_sql.test5
VALUES ('2019-02-11', 'test_1', 23);
INSERT INTO TABLE test_sql.test5
VALUES ('2019-02-11', 'test_2', 19);
INSERT INTO TABLE test_sql.test5
VALUES ('2019-02-11', 'test_3', 39);
INSERT INTO TABLE test_sql.test5
VALUES ('2019-02-11', 'test_1', 23);
INSERT INTO TABLE test_sql.test5
VALUES ('2019-02-11', 'test_3', 39);
INSERT INTO TABLE test_sql.test5
VALUES ('2019-02-11', 'test_1', 23);
INSERT INTO TABLE test_sql.test5
VALUES ('2019-02-12', 'test_2', 19);
INSERT INTO TABLE test_sql.test5
VALUES ('2019-02-13', 'test_1', 23);
INSERT INTO TABLE test_sql.test5
VALUES ('2019-02-15', 'test_2', 19);
INSERT INTO TABLE test_sql.test5
VALUES ('2019-02-16', 'test_2', 19);

--有日志如下，请写出代码求得所有用户和活跃用户的总数及平均年龄。（活跃用户指连续两天都有 访问记录的用户）
--步骤 1 所有用户的总数及平均年龄
with t1 as (
    select distinct
           user_id,
           age
    from test5
),
     t2 as (
         select '所有用户' as type,
                count(user_id) as cnt,
                avg(age)       as avg_age
         from t1
     ),
     --步骤 2 活跃用户的总数及平均年龄,活跃用户指连续两天都有 访问记录的用户）
     t3 as (
         select distinct dt,
                         user_id,
                         age
         from test5
     ),
     t4 as (
         select dt,
                user_id,
                age,
                --同一个客户，按照不同日期排序，得到序号
                row_number() over (partition by user_id order by dt) as rn
         from t3
     ),
     t5 as (
         select *,
                --用日期减去序号得到临时日期
                date_sub(dt,rn) as date2
         from t4
     ),
     t6 as (--统计date2临时日期出现几次。如果2次则表示连续登陆2次
         select user_id,
                date2,
                max(age) age,
                count(1) as cnt
         from t5
         group by user_id,date2
         having count(1)>=2
     ),
     t7 as (
         select distinct user_id,age
         from t6
     ),
     t8 as (
         select '活跃用户'         as type,
                count(user_id) as cnt,
                avg(age)       as avg_age
         from t7
     )
select * from t2 union all
select * from t8;

--第六题

CREATE TABLE test_sql.test6
(
    userid      string,
    money       decimal(10, 2),
    paymenttime string,
    orderid     string);

INSERT INTO TABLE test_sql.test6
VALUES ('001', 100, '2017-10-01', '123'),
       ('001', 200, '2017-10-02', '124'),
       ('002', 500, '2017-10-01', '125'),
       ('001', 100, '2017-11-01', '126');
--请用sql写出所有用户中在今年10月份第一次购买商品的金额，
-- 表ordertable字段:(购买用户：userid，金额：money，购买时间：paymenttime(格式：2017-10-01)， 订单id：orderid
select userid, money, paymenttime
from (select *,
             row_number() over (partition by userid order by paymenttime) rn
      from test6
      where date_format(paymenttime, 'yyyy-MM') = '2017-10') t
where rn = 1;



--（1）创建图书管理库的图书、读者和借阅三个基本表的表结构。请写出建表语句。
-- 创建图书表book*/
CREATE TABLE test_sql.book
(
    book_id   string,
    `SORT`    string,
    book_name string,
    writer    string,
    OUTPUT    string,
    price     decimal(10, 2)
);

INSERT INTO TABLE test_sql.book
VALUES ('001', 'TP391', '信息处理', 'author1', '机械工业出版社', 20),
       ('002', 'TP392', '数据库', 'author12', '科学出版社', 15),
       ('003', 'TP393', '计算机网络', 'author3', '机械工业出版社', 29),
       ('004', 'TP399', '微机原理', 'author4', '科学出版社', 39),
       ('005', 'C931', '管理信息系统', 'author5', '机械工业出版社', 40),
       ('006', 'C932', '运筹学', 'author6', '科学出版社', 55);

-- 创建读者表reader
CREATE TABLE test_sql.reader
(
    reader_id string,
    company   string,
    name      string,
    sex       string,
    grade     string,
    addr      string
);
INSERT overwrite TABLE test_sql.reader
VALUES ('0001', '阿里巴巴', 'jack', '男', 'vp', 'addr1'),
       ('0002', '百度', 'robin', '男', 'vp', 'addr2'),
       ('0003', '腾讯', 'tony', '男', 'vp', 'addr3'),
       ('0004', '京东', 'jasper', '男', 'cfo', 'addr4'),
       ('0005', '网易', 'zhangsan', '女', 'ceo', 'addr5'),
       ('0006', '搜狐', 'lisi', '女', 'ceo', 'addr6'),
       ('0007', '搜狐', '李白', '女', 'ceo', 'addr6');
-- 创建借阅记录表borrow_log
CREATE TABLE test_sql.borrow_log
(
    reader_id   string,
    book_id     string,
    borrow_date string
);
INSERT overwrite TABLE test_sql.borrow_log
VALUES ('0001', '002', '2019-10-14'),
       ('0002', '001', '2019-10-13'),
       ('0003', '005', '2019-09-14'),
       ('0004', '006', '2019-08-15'),
       ('0005', '003', '2019-10-10'),
       ('0005', '004', '2019-10-10'),
       ('0006', '004', '2019-17-13');

-- （2）找出姓李的读者姓名（NAME）和所在单位（COMPANY）。
select name,company from reader where name like '李%';
-- （3）查找“科学出版社”的所有图书名称（BOOK_NAME）及单价（PRICE），结果按单价降序 排序。
select book_name,price from book where OUTPUT='科学出版社';
-- （4）查找价格介于10元和20元之间的图书种类(SORT）出版单位（OUTPUT）和单价（PRICE），
-- 结果按出版单位（OUTPUT）和单价（PRICE）升序排序。
select SORT,OUTPUT,price from book where price >=10 and price<=20 order by OUTPUT,price;
-- （5）查找所有借了书的读者的姓名（NAME）及所在单位（COMPANY）。
select b.name,b.company from borrow_log a
join reader b on a.reader_id=b.reader_id;
-- （6）求”科学出版社”图书的最高单价、最低单价、平均单价。
select max(price),min(price),avg(price) from book where OUTPUT='科学出版社';
-- （7）找出当前至少借阅了2本图书（大于等于2本）的读者姓名及其所在单位。
select b.reader_id,b.name, b.company
from borrow_log a
join reader b on a.reader_id=b.reader_id
group by b.reader_id,b.name, b.company
having count(*)>=2
;
-- （8）考虑到数据安全的需要，需定时将“借阅记录”中数据进行备份，请使用一条SQL语句，
-- 在备份 用户bak下创建与“借阅记录”表结构完全一致的数据表 BORROW_LOG_BAK.
-- 井且将“借阅记录”中现 有数据全部复制到BORROW_L0G_ BAK中。
create table BORROW_LOG_BAK as select * from borrow_log;
-- （9）现在需要将原Oracle数据库中数据迁移至Hive仓库，
-- 请写出“图书”在Hive中的建表语句（Hive 实现，提示：列分隔符|；数据表数据需要外部导入：分区分别以month＿part、day＿part 命名）
CREATE TABLE book_hive(
    book_id   string,
    SORT      string,
    book_name string,
    writer    string,
    OUTPUT    string,
    price     DECIMAL(10, 2)
)
    partitioned BY ( month_part string, day_part string )
    ROW format delimited FIELDS TERMINATED BY '\\|' stored AS textfile;
-- （10）Hive中有表A，现在需要将表A的月分区 201505 中 user＿id为20000的user＿dinner字段更新为 bonc8920，
-- 其他用户user＿dinner字段数据不变，请列出更新的方法步骤。
-- （Hive实现， 提示：Hlive中无update语法，请通过其他办法进行数据更新）

create or replace temporary view A1 (user_id,user_dinner) as values
(20000,'bonc8888'),
(10000,'bonc7777'),
(30000,'bonc9999');
select * from A1;
create table A as select * from A1;
select * from A;

insert overwrite table A
select user_id,'bonc8920' as user_dinner  from A where user_id=20000
union all
select * from A where user_id!=20000

--第8题
CREATE TABLE test_sql.test8
(
    `date`    string,
    interface string,
    ip        string
);
INSERT INTO TABLE test_sql.test8
VALUES ('2016-11-09 11:22:05', '/api/user/login', '110.23.5.23');
INSERT INTO TABLE test_sql.test8
VALUES ('2016-11-09 11:23:10', '/api/user/detail', '57.3.2.16');
INSERT INTO TABLE test_sql.test8
VALUES ('2016-11-09 23:59:40', '/api/user/login', '200.6.5.166');
INSERT INTO TABLE test_sql.test8
VALUES ('2016-11-09 11:14:23', '/api/user/login', '136.79.47.70');
INSERT INTO TABLE test_sql.test8
VALUES ('2016-11-09 11:15:23', '/api/user/detail', '94.144.143.141');
INSERT INTO TABLE test_sql.test8
VALUES ('2016-11-09 11:16:23', '/api/user/login', '197.161.8.206');
INSERT INTO TABLE test_sql.test8
VALUES ('2016-11-09 12:14:23', '/api/user/detail', '240.227.107.145');
INSERT INTO TABLE test_sql.test8
VALUES ('2016-11-09 13:14:23', '/api/user/login', '79.130.122.205');
INSERT INTO TABLE test_sql.test8
VALUES ('2016-11-09 14:14:23', '/api/user/detail', '65.228.251.189');
INSERT INTO TABLE test_sql.test8
VALUES ('2016-11-09 14:15:23', '/api/user/detail', '245.23.122.44');
INSERT INTO TABLE test_sql.test8
VALUES ('2016-11-09 14:17:23', '/api/user/detail', '22.74.142.137');
INSERT INTO TABLE test_sql.test8
VALUES ('2016-11-09 14:19:23', '/api/user/detail', '54.93.212.87');
INSERT INTO TABLE test_sql.test8
VALUES ('2016-11-09 14:20:23', '/api/user/detail', '218.15.167.248');
INSERT INTO TABLE test_sql.test8
VALUES ('2016-11-09 14:24:23', '/api/user/detail', '20.117.19.75');
INSERT INTO TABLE test_sql.test8
VALUES ('2016-11-09 15:14:23', '/api/user/login', '183.162.66.97');
INSERT INTO TABLE test_sql.test8
VALUES ('2016-11-09 16:14:23', '/api/user/login', '108.181.245.147');
INSERT INTO TABLE test_sql.test8
VALUES ('2016-11-09 14:17:23', '/api/user/login', '22.74.142.137');
INSERT INTO TABLE test_sql.test8
VALUES ('2016-11-09 14:19:23', '/api/user/login', '22.74.142.137');

--求11月9号下午14点（14-15点），访问/api/user/login接口的top10的ip地址
select ip,
       count(*)
from test8
where date >= '2016-11-09 14:00:00'
  and date < '2016-11-09 15:00:00'
  and interface='/api/user/login'
group by ip
order by count(*) desc
limit 10;


--第九题
CREATE TABLE test_sql.test9
(
    dist_id     string COMMENT '区组id',
    account     string COMMENT '账号',
    `money`     decimal(10, 2) COMMENT '充值金额',
    create_time string COMMENT '订单时间'
);
INSERT INTO TABLE test_sql.test9 VALUES ('1','11',100006,'2019-01-02 13:00:01'); INSERT INTO TABLE test_sql.test9 VALUES ('1','22',110000,'2019-01-02 13:00:02'); INSERT INTO TABLE test_sql.test9 VALUES ('1','33',102000,'2019-01-02 13:00:03'); INSERT INTO TABLE test_sql.test9 VALUES ('1','44',100300,'2019-01-02 13:00:04'); INSERT INTO TABLE test_sql.test9 VALUES ('1','55',100040,'2019-01-02 13:00:05'); INSERT INTO TABLE test_sql.test9 VALUES ('1','66',100005,'2019-01-02 13:00:06'); INSERT INTO TABLE test_sql.test9 VALUES ('1','77',180000,'2019-01-03 13:00:07'); INSERT INTO TABLE test_sql.test9 VALUES ('1','88',106000,'2019-01-02 13:00:08'); INSERT INTO TABLE test_sql.test9 VALUES ('1','99',100400,'2019-01-02 13:00:09'); INSERT INTO TABLE test_sql.test9 VALUES ('1','12',100030,'2019-01-02 13:00:10'); INSERT INTO TABLE test_sql.test9 VALUES ('1','13',100003,'2019-01-02 13:00:20'); INSERT INTO TABLE test_sql.test9 VALUES ('1','14',100020,'2019-01-02 13:00:30'); INSERT INTO TABLE test_sql.test9 VALUES ('1','15',100500,'2019-01-02 13:00:40'); INSERT INTO TABLE test_sql.test9 VALUES ('1','16',106000,'2019-01-02 13:00:50'); INSERT INTO TABLE test_sql.test9 VALUES ('1','17',100800,'2019-01-02 13:00:59'); INSERT INTO TABLE test_sql.test9 VALUES ('2','18',100800,'2019-01-02 13:00:11'); INSERT INTO TABLE test_sql.test9 VALUES ('2','19',100030,'2019-01-02 13:00:12'); INSERT INTO TABLE test_sql.test9 VALUES ('2','10',100000,'2019-01-02 13:00:13'); INSERT INTO TABLE test_sql.test9 VALUES ('2','45',100010,'2019-01-02 13:00:14'); INSERT INTO TABLE test_sql.test9 VALUES ('2','78',100070,'2019-01-02 13:00:15');

select *
from (select *,
             row_number() over (distribute by dist_id order by money desc) as rn
      from test9
      where to_date(create_time) = '2019-01-02') t
where rn = 1;
;

--第十题
CREATE TABLE test_sql.test10
(
    `dist_id` string COMMENT '区组id',
    `account` string COMMENT '账号',
    `gold`    int COMMENT '金币'
);
INSERT INTO TABLE test_sql.test10
VALUES ('1', '77', 18);
INSERT INTO TABLE test_sql.test10
VALUES ('1', '88', 106);
INSERT INTO TABLE test_sql.test10
VALUES ('1', '99', 10);
INSERT INTO TABLE test_sql.test10
VALUES ('1', '12', 13);
INSERT INTO TABLE test_sql.test10
VALUES ('1', '13', 14);
INSERT INTO TABLE test_sql.test10
VALUES ('1', '14', 25);
INSERT INTO TABLE test_sql.test10
VALUES ('1', '15', 36);
INSERT INTO TABLE test_sql.test10
VALUES ('1', '16', 12);
INSERT INTO TABLE test_sql.test10
VALUES ('1', '17', 158);
INSERT INTO TABLE test_sql.test10
VALUES ('2', '18', 12);
INSERT INTO TABLE test_sql.test10
VALUES ('2', '19', 44);
INSERT INTO TABLE test_sql.test10
VALUES ('2', '10', 66);
INSERT INTO TABLE test_sql.test10
VALUES ('2', '45', 80);
INSERT INTO TABLE test_sql.test10
VALUES ('2', '78', 98);

select dist_id, account, gold
from (select *,
             row_number() over (partition by dist_id order by gold desc) rn
      from test_sql.test10) t
where rn <= 10
;














