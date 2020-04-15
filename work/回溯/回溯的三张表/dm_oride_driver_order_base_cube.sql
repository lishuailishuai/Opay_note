dm_oride_driver_order_base_cube        dt=2019-11-15
	dwm_oride_driver_order_base_di     dt=2019-06-15
		dwm_oride_order_base_di
			dwd_oride_order_base_include_test_di --no
			dwd_oride_order_assign_driver_detail_di --no
			dwd_oride_order_push_driver_detail_di --no
			dwd_oride_driver_accept_order_show_detail_di --no
			dwd_oride_driver_accept_order_click_detail_di --no
			dwd_oride_order_mark_df
				dwd_oride_order_base_include_test_di
				dim_oride_city
				weather_per_10min
				data_user_comment     dt=2019-06-15


一个月一个月执行，弄成多个脚本
=============================

datebeg="2019-06-15"
dateend="2019-11-15"

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

    INSERT overwrite TABLE oride_dw.dm_oride_driver_order_base_cube partition(country_code,dt)

    select nvl(city_id,-10000) as city_id,

       nvl(product_id,-10000) as product_id,
       
       td_request_driver_num,
       --当日接单司机数
       
       td_finish_order_driver_num,
       --当日完单司机数
       
       driver_request_order_cnt,
       --司机接单量（理论和应答量一样）
       
       driver_finish_order_cnt,
       --司机完单量
       
       driver_finished_pay_order_cnt,
       --司机支付完单量
       
       driver_finish_price,
       --司机完单gmv
       
       driver_billing_dur,
       --司机计费时长
       
       driver_service_dur,
       --司机服务时长
       
       driver_finished_dur,
       --司机支付完单做单时长（支付跨天可能偏大）
       
       driver_cannel_pick_dur,
       -- 司机当天订单被取消时长,不可用于计算司机在线时长 
       
       country_code,
       
       '${day}' as dt              
from (select city_id,

           product_id,
           --下单业务类型
           
           count(distinct if(driver_request_order_cnt>=1,driver_id,null)) as td_request_driver_num,
           --当日接单司机数
           
           count(distinct if(driver_finish_order_cnt>=1,driver_id,null)) as td_finish_order_driver_num,
           --当日完单司机数
           
           sum(driver_request_order_cnt) as driver_request_order_cnt,
           --司机接单量（理论和应答量一样）
           
           sum(driver_finish_order_cnt) as driver_finish_order_cnt,
           --司机完单量
           
           sum(driver_finished_pay_order_cnt) as driver_finished_pay_order_cnt,
           --司机支付完单量
           
           sum(driver_finish_price) as driver_finish_price,
           --司机完单gmv
           
           sum(driver_billing_dur) as driver_billing_dur,
           --司机计费时长
           
           sum(driver_service_dur) as driver_service_dur,
           --司机服务时长
           
           sum(driver_finished_dur) as driver_finished_dur,
           --司机支付完单做单时长（支付跨天可能偏大）
           
           sum(driver_cannel_pick_dur) as driver_cannel_pick_dur,
           -- 司机当天订单被取消时长,不可用于计算司机在线时长
           
           nvl(country_code,'-999') as country_code
           --二位国家码  --(去除with cube为空的BUG)
              
        from oride_dw.dwm_oride_driver_order_base_di
        where dt='${day}'   
        group by city_id,
        
               product_id,
               --下单业务类型 
                 
               country_code
               --二位国家码
               
               with cube) t
        where t.country_code<>'-999';   
	"
    beg_s=$((beg_s+86400));
done

echo "日期全部处理完成"


==============================

    




