dm_oride_passenger_base_multi_cube_d      dt=2019-09-04
	dwd_oride_order_base_include_test_di  dt=2019-06-15
	dwd_oride_order_base_include_test_df
	dwd_oride_order_base_include_test_df
	dim_oride_passenger_base          dt=2019-07-23
		ods_sqoop_base_data_user_df   dt=2019-07-23
		ods_sqoop_base_data_user_extend_df  dt=2019-08-13
回溯历史数据。06-15------09-04    回溯成功，显示07-05------至今   已经验证07-05之前没有数据，并且补全历史数据






airflow backfill -x --rerun_failed_tasks -t dm_oride_passenger_base_multi_cube_d_task -s 2019-06-15 -e 2019-07-15 dm_oride_passenger_base_multi_cube_d

airflow backfill -x --rerun_failed_tasks -t dm_oride_passenger_base_multi_cube_d_task -s 2019-07-15 -e 2019-08-14 dm_oride_passenger_base_multi_cube_d

airflow backfill -x --rerun_failed_tasks -t dm_oride_passenger_base_multi_cube_d_task -s 2019-08-14 -e 2019-09-04 dm_oride_passenger_base_multi_cube_d


hdfs dfs -ls ufile://opay-datalake/oride/oride_dw/dwd_oride_order_base_include_test_df/country_code=nal/dt=2019-06-15



ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCXKVlmTk/n1CzjG2tpu4TTyhbF1jbss+VA37SthGV9CK7ceIGZObWc9YbAgZbWfDn1nk5ogH+St0vraECsMs+j7bv52XygGwOMxR7Ezh+WVNa3IIQA3oB+Ub1c8ZUEdpebNQqqG4mn4v2GzkxEDIzYahr95jwjPeSsmUuPgv+RrbmNsuS2Tz154NhOp3+pxfDw7g9PbTzOkZZecKRAPkwOxmzDifFJprE9zqluU5M+KktAouKQvu5RyZ69qwODuNRqSxOmbDYBE3gwXlKZTYTHIruW1kuz5qlQsJnSVPM7259XdxoYLWRKxZVa890FREMOinZqL/JplJ7VMPN57hsL lishuai@lishuaideMacBook-Pro.local







dm_oride_passenger_base_multi_cube_d      dt=2019-09-04
	dwd_oride_order_base_include_test_di  dt=2019-06-15
	dwd_oride_order_base_include_test_df
	dwd_oride_order_base_include_test_df
	dim_oride_passenger_base          dt=2019-07-23
		ods_sqoop_base_data_user_df   dt=2019-07-23
		ods_sqoop_base_data_user_extend_df  dt=2019-08-13


================================

dm_oride_passenger_base_multi_cube_d


=============================

datebeg="2019-06-15"
dateend="2019-09-05"

beg_s=`date -d "$datebeg" +%s`
end_s=`date -d "$dateend" +%s`




while [ "$beg_s" -le "$end_s" ];do
    day=`date -d @$beg_s +"%Y-%m-%d"`;
    month=${day%-*};
    startMonth=${month}-01
    echo "当前日期：$day"

    hive -e "
		set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    with order_base_data as (
          SELECT if(t2.passenger_id IS NULL,1,0) AS is_first_order_mark,--准确说历史没有完单的是否本日首次
             if(t3.passenger_id IS NOT NULL,1,0) AS new_reg_user_mark, --是否当日新注册乘客
             t1.*
            FROM
              (SELECT *
               FROM oride_dw.dwd_oride_order_base_include_test_di
               WHERE dt='${day}'
               and city_id<>'999001' --去除测试数据
               and driver_id<>1) t1
            LEFT JOIN
              (SELECT passenger_id
               FROM oride_dw.dwd_oride_order_base_include_test_df
               WHERE dt in('2019-11-25','his')
                 AND create_date<dt
                 AND status IN(4,
                               5)
               GROUP BY passenger_id) t2 ON t1.passenger_id=t2.passenger_id
            LEFT JOIN
              (SELECT *
               FROM oride_dw.dim_oride_passenger_base
               WHERE dt='2019-11-25'
                 AND substr(register_time,1,10)=dt) t3 ON t1.passenger_id=t3.passenger_id
            inner join
              (select city_id,product_id,size(split(product_id,',')) as product_id_cnt
                from oride_dw.dim_oride_city
                where dt='2019-11-25' and size(split(product_id,','))>1) multi_cit
                on t1.city_id=multi_cit.city_id
                    )
    INSERT overwrite TABLE oride_dw.dm_oride_passenger_base_multi_cube_d partition(country_code,dt)
    select 
        nvl(t2.city_id,-10000) as city_id,
        nvl(t2.product_id,-10000) as product_id,
        nvl(t1.new_users,0) as new_users,  --当天注册乘客数
        nvl(t1.act_users,0) as act_users,  --当天活跃乘客数
        nvl(t2.ord_users,0) as ord_users,  --当日下单乘客数
        nvl(t2.finished_users,0) as finished_users,  --当日完单乘客数
        nvl(t2.first_finished_users,0) as first_finished_users,  --当日订单中首次完单乘客数
        nvl(t2.old_finished_users,0) as old_finished_users,   --当日订单中完单老客数
        nvl(t2.new_user_ord_cnt,0) as new_user_ord_cnt,  --当日新注册乘客下单量
        nvl(t2.new_user_finished_cnt,0) as new_user_finished_cnt,  --当日新注册乘客完单量
        nvl(t2.new_user_gmv,0.0) as new_user_gmv,  --当日注册乘客完单gmv
        nvl(t2.paid_users,0) as paid_users,  --当日所有支付乘客数
        nvl(t2.online_paid_users,0) as online_paid_users,--当日线上支付乘客数
        nvl(t2.fraud_user_cnt,0) as fraud_user_cnt, --疑似作弊订单乘客数
        nvl(t2.driver_serv_type,-10000) as driver_serv_type, --订单表中司机业务类型
        nvl(t2.country_code,'nal') as country_code,
        '${day}' dt     
        from (SELECT 'nal' AS country_code,
               -10000 AS city_id,
               -10000 AS product_id,
               -10000 as driver_serv_type,
               count(if(substr(register_time,1,10)=dt,passenger_id,NULL)) AS new_users, --当天注册乘客数
               count(if(substr(login_time,1,10)=dt,passenger_id,NULL)) AS act_users --当天活跃乘客数
        FROM oride_dw.dim_oride_passenger_base
        WHERE dt='2019-11-25') t1

        right join

        (SELECT nvl(country_code,'-10000') as country_code,
               city_id,
               product_id, --招手停订单数限定具体业务线
               driver_serv_type, --订单表中对应的司机业务类型
         count(DISTINCT passenger_id) AS ord_users, --当日下单乘客数
         count(DISTINCT (if(status IN(4,5),passenger_id,NULL))) AS finished_users, --当日完单乘客数
         count(DISTINCT (IF (status IN(4,5)
                             AND is_first_order_mark=1,passenger_id,NULL))) AS first_finished_users, --当日订单中首次完单乘客数
         count(DISTINCT (IF (status IN(4,5)
                             AND is_first_order_mark=0,passenger_id,NULL))) AS old_finished_users, --当日订单中完单老客数
         count(IF (new_reg_user_mark=1,order_id,NULL)) AS new_user_ord_cnt, --当日新注册乘客下单量
         count(IF (new_reg_user_mark=1
                   AND status IN(4,5),order_id,NULL)) AS new_user_finished_cnt, --当日新注册乘客完单量
         sum(IF (new_reg_user_mark=1
                   AND status in(4,5),price,0.0)) AS new_user_gmv, --当日注册乘客完单gmv
         count(distinct(IF (pay_status=1,passenger_id,NULL))) AS paid_users, --当日所有支付乘客数
         count(distinct(IF (pay_status=1
                            AND pay_mode IN(2,3),passenger_id,NULL))) AS online_paid_users, --当日线上支付乘客数
         null as fraud_user_cnt --疑似作弊订单乘客数
        FROM order_base_data 
        group by nvl(country_code,'-10000'),
               city_id,
               product_id,
               driver_serv_type
        with cube) t2
        on t1.country_code=t2.country_code and t1.city_id=nvl(t2.city_id,-10000) and t1.product_id=nvl(t2.product_id,-10000)
        and t1.driver_serv_type=nvl(t2.driver_serv_type,-10000)
        where t2.country_code<>'-10000';

	"
    beg_s=$((beg_s+86400));
done

echo "日期全部处理完成"


        ==============================






