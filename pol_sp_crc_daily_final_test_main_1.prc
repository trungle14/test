create or replace procedure pol_sp_crc_daily_final_test_main_1 as
begin


drop_table_if_exists('pol_tru_crc_main_daily_'||to_char(sysdate,'ddmmyy')||'');
execute immediate 'create table pol_tru_crc_main_daily_'||to_char(sysdate,'ddmmyy')||' as
select * from (
select/*+ parallel(+32)*/ a.*, t.due_date, total_amount_due as min_due_amt
from sdm.sdm_col_balance_crc a
left join sdm.sdm_col_statement_crc t on a.appl_id = t.appl_id  and a.last_stmt_date = t.STATEMENT_DATE
where total_amount_due >0
and status = ''A''
and run_date = trunc(sysdate)
and STATEMENT_DATE = trunc(sysdate)-5
and a.dpd  = 0  )
where due_date = run_date + 10';





insert into pol_tbl_predue_crc_job_control_test
(select 'pol_tru_crc_main_daily',sysdate,'Ready' from dual);
commit;

drop_table_if_exists('pol_tru_crc_main_daily_1');
execute immediate 'create table pol_tru_crc_main_daily_1 as
select /*+ parallel(+32)*/ a.*, account_number, id_no, personal_expense, real_income, (real_income-personal_expense) saving,
case when dcard_fg = 1 or lsm_schemeid = ''3650'' then 1 else 0 end as dcard_fg
 from pol_tru_crc_main_daily_'||to_char(sysdate,'ddmmyy')||' a
left join sdm.sdm_col_account_crc t on a.appl_id = t.appl_id
left join sdm.mv_apps_all x on a.appl_id = x.app_id';

insert into pol_tbl_predue_crc_job_control_test
(select 'pol_tru_crc_main_daily_1',sysdate,'Ready' from dual);
commit;


drop_table_if_exists('pol_predue_crc_main_daily_cash_act');
execute immediate 'create table pol_predue_crc_main_daily_cash_act as
select/*+PARALLEL(+32)*/ a.appl_id, run_date,
case when  t.transaction_code in (select transaction_code from cicdata.MV_CRC_TRANS_CD
  where type IN (''CASH'',''REV CASH'')) and effect_date between  add_months(run_date,-3) and run_date  then 1 else 0 end as cash_3m,
case when  t.transaction_code in (select transaction_code from cicdata.MV_CRC_TRANS_CD
  where type IN (''CASH'',''REV CASH'')) and effect_date between add_months(run_date,-6) and run_date  then 1 else 0 end as cash_6m,
case when  t.transaction_code in (select transaction_code from cicdata.MV_CRC_TRANS_CD
  where type IN (''CASH'',''REV CASH'')) and effect_date between add_months(run_date,-9) and run_date then 1 else 0 end as cash_9m,
case when  t.transaction_code in (select transaction_code from cicdata.MV_CRC_TRANS_CD
  where type IN (''CASH'',''REV CASH'')) and effect_date between add_months(run_date,-3) and run_date
  and actived_dt between add_months(run_date,-3) and run_date  then 1 else 0 end as cash_act_3m,
case when  t.transaction_code in (select transaction_code from cicdata.MV_CRC_TRANS_CD
  where type IN (''CASH'',''REV CASH'')) and effect_date between add_months(run_date,-6) and run_date
  and actived_dt between add_months(run_date,-6) and run_date  then 1 else 0 end as cash_act_6m,
case when  t.transaction_code in (select transaction_code from cicdata.MV_CRC_TRANS_CD
  where type IN (''CASH'',''REV CASH'')) and effect_date between add_months(run_date,-9) and run_date
  and actived_dt between add_months(run_date,-9) and run_date  then 1 else 0 end as cash_act_9m,
case when  t.transaction_code in (select transaction_code from cicdata.MV_CRC_TRANS_CD
  where type IN (''CASH'',''REV CASH'')) and effect_date between add_months(run_date,-3) and run_date
  and actived_dt between add_months(run_date,-3) and run_date  then amount else 0 end as cash_act_amt_3m,
case when  t.transaction_code in (select transaction_code from cicdata.MV_CRC_TRANS_CD
  where type IN (''CASH'',''REV CASH'')) and effect_date between add_months(run_date,-6) and run_date
  and actived_dt between add_months(run_date,-6) and run_date  then amount else 0 end as cash_act_amt_6m,
case when  t.transaction_code in (select transaction_code from cicdata.MV_CRC_TRANS_CD
  where type IN (''CASH'',''REV CASH'')) and effect_date between add_months(run_date,-9) and run_date
  and actived_dt between add_months(run_date,-9) and run_date  then amount else 0 end as cash_act_amt_9m,

case when  t.transaction_code in (select transaction_code from cicdata.MV_CRC_TRANS_CD
  where type IN (''VNPOS FOR IPP'', ''REV VNPOS FOR IPP'',''FAST CASH'',''REV FAST CASH'')) and effect_date between  add_months(run_date,-3) and run_date  then 1 else 0 end as ipp_3m,
case when  t.transaction_code in (select transaction_code from cicdata.MV_CRC_TRANS_CD
  where type IN (''VNPOS FOR IPP'', ''REV VNPOS FOR IPP'',''FAST CASH'',''REV FAST CASH'')) and effect_date between add_months(run_date,-6) and run_date  then 1 else 0 end as ipp_6m,
case when  t.transaction_code in (select transaction_code from cicdata.MV_CRC_TRANS_CD
  where type IN (''VNPOS FOR IPP'', ''REV VNPOS FOR IPP'',''FAST CASH'',''REV FAST CASH''))  and effect_date between add_months(run_date,-9) and run_date then 1 else 0 end as ipp_9m,
case when  t.transaction_code in (select transaction_code from cicdata.MV_CRC_TRANS_CD
  where type IN (''VNPOS FOR IPP'', ''REV VNPOS FOR IPP'',''FAST CASH'',''REV FAST CASH''))  and effect_date between add_months(run_date,-3) and run_date
  and actived_dt between add_months(run_date,-3) and run_date  then 1 else 0 end as ipp_act_3m,
case when  t.transaction_code in (select transaction_code from cicdata.MV_CRC_TRANS_CD
  where type IN (''VNPOS FOR IPP'', ''REV VNPOS FOR IPP'',''FAST CASH'',''REV FAST CASH''))  and effect_date between add_months(run_date,-6) and run_date
  and actived_dt between add_months(run_date,-6) and run_date  then 1 else 0 end as ipp_act_6m,
case when  t.transaction_code in (select transaction_code from cicdata.MV_CRC_TRANS_CD
  where type IN (''VNPOS FOR IPP'', ''REV VNPOS FOR IPP'',''FAST CASH'',''REV FAST CASH''))  and effect_date between add_months(run_date,-9) and run_date
  and actived_dt between add_months(run_date,-9) and run_date  then 1 else 0 end as ipp_act_9m,
case when  t.transaction_code in (select transaction_code from cicdata.MV_CRC_TRANS_CD
where type IN (''VNPOS FOR IPP'', ''REV VNPOS FOR IPP'',''FAST CASH'',''REV FAST CASH''))  and effect_date between add_months(run_date,-3) and run_date
  and actived_dt between add_months(run_date,-3) and run_date  then amount else 0 end as ipp_act_amt_3m,
case when  t.transaction_code in (select transaction_code from cicdata.MV_CRC_TRANS_CD
  where type IN (''VNPOS FOR IPP'', ''REV VNPOS FOR IPP'',''FAST CASH'',''REV FAST CASH'')) and effect_date between add_months(run_date,-6) and run_date
  and actived_dt between add_months(run_date,-6) and run_date  then amount else 0 end as ipp_act_amt_6m,
case when  t.transaction_code in (select transaction_code from cicdata.MV_CRC_TRANS_CD
  where type IN (''VNPOS FOR IPP'', ''REV VNPOS FOR IPP'',''FAST CASH'',''REV FAST CASH''))  and effect_date between add_months(run_date,-9) and run_date
  and actived_dt between add_months(run_date,-9) and run_date  then amount else 0 end as ipp_act_amt_9m
from pol_tru_crc_main_daily_1 a
left join sdm.sdm_col_transaction_crc t on a.appl_id = t.appl_id
left join common.Pol_tbl_tran_cc_portfolio e on a.appl_id = e.app_id
where effect_date between add_months(run_date,-10) and  run_date';


insert into pol_tbl_predue_crc_job_control_test
(select 'pol_predue_crc_main_daily_cash_act',sysdate,'Ready' from dual);
commit;

drop_table_if_exists('pol_predue_crc_main_daily_cash_act_amt') ;
execute immediate 'create table pol_predue_crc_main_daily_cash_act_amt as
select /*+PARALLEL(+32)*/appl_id, run_date,
sum(cash_3m)   s_cash_3m,
sum(cash_6m)   s_cash_6m,
sum(cash_9m)   s_cash_9m,
sum(cash_act_3m)   s_cash_act_3m,
sum(cash_act_6m)   s_cash_act_6m,
sum(cash_act_9m)   s_cash_act_9m,
sum(cash_act_amt_3m)   s_cash_act_amt_3m,
sum(cash_act_amt_6m)   s_cash_act_amt_6m,
sum(cash_act_amt_9m)   s_cash_act_amt_9m,
max(cash_3m)   m_cash_3m,
max(cash_6m)   m_cash_6m,
max(cash_9m)   m_cash_9m,
max(cash_act_3m)   m_cash_act_3m,
max(cash_act_6m)   m_cash_act_6m,
max(cash_act_9m)   m_cash_act_9m,
max(cash_act_amt_3m)   m_cash_act_amt_3m,
max(cash_act_amt_6m)   m_cash_act_amt_6m,
max(cash_act_amt_9m)   m_cash_act_amt_9m,
sum(ipp_3m)   s_ipp_3m,
sum(ipp_6m)   s_ipp_6m,
sum(ipp_9m)   s_ipp_9m,
sum(ipp_act_3m)   s_ipp_act_3m,
sum(ipp_act_6m)   s_ipp_act_6m,
sum(ipp_act_9m)   s_ipp_act_9m,
sum(ipp_act_amt_3m)   s_ipp_act_amt_3m,
sum(ipp_act_amt_6m)   s_ipp_act_amt_6m,
sum(ipp_act_amt_9m)   s_ipp_act_amt_9m,
max(ipp_3m)   m_ipp_3m,
max(ipp_6m)   m_ipp_6m,
max(ipp_9m)   m_ipp_9m,
max(ipp_act_3m)   m_ipp_act_3m,
max(ipp_act_6m)   m_ipp_act_6m,
max(ipp_act_9m)   m_ipp_act_9m,
max(ipp_act_amt_3m)   m_ipp_act_amt_3m,
max(ipp_act_amt_6m)   m_ipp_act_amt_6m,
max(ipp_act_amt_9m)   m_ipp_act_amt_9m

 from  pol_predue_crc_main_daily_cash_act
group by appl_id, run_date';


insert into pol_tbl_predue_crc_job_control_test
(select 'pol_predue_crc_main_daily_cash_act_amt',sysdate,'Ready' from dual);
commit;

drop_table_if_exists('pol_predue_crc_main_daily_col') ;
execute immediate 'create table pol_predue_crc_main_daily_col as
select /*+ parallel(+32)*/a.appl_id,a.run_date
       , nvl(sum(BAD_RESPONSE),0) BAD_RESPONSE_CNT
       , nvl(sum(case when months_between(run_date, f.contact_month) <=3 then BAD_RESPONSE end),0) BAD_RESPONSE_3M_CNT
       , nvl(sum(case when months_between(run_date, f.contact_month) <=6 then BAD_RESPONSE end),0) BAD_RESPONSE_6M_CNT
       , nvl(sum(case when months_between(run_date, f.contact_month) <=9 then BAD_RESPONSE end),0) BAD_RESPONSE_9M_CNT
       , nvl(sum(case when months_between(run_date, f.contact_month) <=12 then BAD_RESPONSE end),0) BAD_RESPONSE_12M_CNT
       , nvl(sum(case when months_between(run_date, f.contact_month) <=24 then BAD_RESPONSE end),0) BAD_RESPONSE_24M_CNT

       , nvl(sum(GOOD_RESPONSE),0) GOOD_RESPONSE_CNT
       , nvl(sum(case when months_between(run_date, f.contact_month) <=3 then GOOD_RESPONSE end),0) GOOD_RESPONSE_3M_CNT
       , nvl(sum(case when months_between(run_date, f.contact_month) <=6 then GOOD_RESPONSE end),0) GOOD_RESPONSE_6M_CNT
       , nvl(sum(case when months_between(run_date, f.contact_month) <=9 then GOOD_RESPONSE end),0) GOOD_RESPONSE_9M_CNT
       , nvl(sum(case when months_between(run_date, f.contact_month) <=12 then GOOD_RESPONSE end),0) GOOD_RESPONSE_12M_CNT
       , nvl(sum(case when months_between(run_date, f.contact_month) <=24 then GOOD_RESPONSE end),0) GOOD_RESPONSE_24M_CNT

       , nvl(sum(ATTEMPT_CNT),0) ATTEMPT_CNT
       , nvl(sum(case when months_between(run_date, f.contact_month) <=3 then ATTEMPT_CNT end),0) ATTEMPT_3M_CNT
       , nvl(sum(case when months_between(run_date, f.contact_month) <=6 then ATTEMPT_CNT end),0) ATTEMPT_6M_CNT
       , nvl(sum(case when months_between(run_date, f.contact_month) <=9 then ATTEMPT_CNT end),0) ATTEMPT_9M_CNT
       , nvl(sum(case when months_between(run_date, f.contact_month) <=12 then ATTEMPT_CNT end),0) ATTEMPT_12M_CNT
       , nvl(sum(case when months_between(run_date, f.contact_month) <=24 then ATTEMPT_CNT end),0) ATTEMPT_24M_CNT

       , nvl(sum(CONNECT_CNT),0) CONNECT_CNT
       , nvl(sum(case when months_between(run_date, f.contact_month) <=3 then CONNECT_CNT end),0) CONNECT_3M_CNT
       , nvl(sum(case when months_between(run_date, f.contact_month) <=6 then CONNECT_CNT end),0) CONNECT_6M_CNT
       , nvl(sum(case when months_between(run_date, f.contact_month) <=9 then CONNECT_CNT end),0) CONNECT_9M_CNT
       , nvl(sum(case when months_between(run_date, f.contact_month) <=12 then CONNECT_CNT end),0) CONNECT_12M_CNT
       , nvl(sum(case when months_between(run_date, f.contact_month) <=24 then CONNECT_CNT end),0) CONNECT_24M_CNT

       , nvl(sum(CONTACT_CLIENT_CNT),0) CONTACT_CLIENT_CNT
       , nvl(sum(case when months_between(run_date, f.contact_month) <=3 then CONTACT_CLIENT_CNT end),0) CONTACT_CLIENT_3M_CNT
       , nvl(sum(case when months_between(run_date, f.contact_month) <=6 then CONTACT_CLIENT_CNT end),0) CONTACT_CLIENT_6M_CNT
       , nvl(sum(case when months_between(run_date, f.contact_month) <=9 then CONTACT_CLIENT_CNT end),0) CONTACT_CLIENT_9M_CNT
       , nvl(sum(case when months_between(run_date, f.contact_month) <=12 then CONTACT_CLIENT_CNT end),0) CONTACT_CLIENT_12M_CNT
       , nvl(sum(case when months_between(run_date, f.contact_month) <=24 then CONTACT_CLIENT_CNT end),0) CONTACT_CLIENT_24M_CNT
       , nvl(min(case when CONTACT_CLIENT_CNT >= 1 then months_between(run_date, f.contact_month) end),99) CONTACT_CLIENT_LAST
       , nvl(max(case when CONTACT_CLIENT_CNT >= 1 then months_between(run_date, f.contact_month) end),99) CONTACT_CLIENT_FIRST

       , nvl(sum(case when months_between(run_date, f.contact_month) <=3 and CONTACT_CLIENT_CNT > 1 then 1 end),0) MONTH_CONTACT_CLIENT_3M
       , nvl(sum(case when months_between(run_date, f.contact_month) <=6 and CONTACT_CLIENT_CNT > 1 then 1 end),0) MONTH_CONTACT_CLIENT_6M
       , nvl(sum(case when months_between(run_date, f.contact_month) <=9 and CONTACT_CLIENT_CNT > 1 then 1 end),0) MONTH_CONTACT_CLIENT_9M
       , nvl(sum(case when months_between(run_date, f.contact_month) <=12 and CONTACT_CLIENT_CNT > 1 then 1 end),0) MONTH_CONTACT_CLIENT_12M
       , nvl(sum(case when months_between(run_date, f.contact_month) <=24 and CONTACT_CLIENT_CNT > 1 then 1 end),0) MONTH_CONTACT_CLIENT_24M

       , nvl(sum(CONTACT_CNT),0) CONTACT_CNT
       , nvl(sum(case when months_between(run_date, f.contact_month) <=3 then CONTACT_CNT end),0) CONTACT_3M_CNT
       , nvl(sum(case when months_between(run_date, f.contact_month) <=6 then CONTACT_CNT end),0) CONTACT_6M_CNT
       , nvl(sum(case when months_between(run_date, f.contact_month) <=9 then CONTACT_CNT end),0) CONTACT_9M_CNT
       , nvl(sum(case when months_between(run_date, f.contact_month) <=12 then CONTACT_CNT end),0) CONTACT_12M_CNT
       , nvl(sum(case when months_between(run_date, f.contact_month) <=24 then CONTACT_CNT end),0) CONTACT_24M_CNT


       , round(nvl(sum(BAD_RESPONSE)/sum(GOOD_RESPONSE + BAD_RESPONSE + 0.01),0),4) BAD_GOOD_RESPONSE_CNT
       , round(nvl(sum(case when months_between(run_date, f.contact_month) <=3 then BAD_RESPONSE end)/
                  sum(case when months_between(run_date, f.contact_month) <=3 then BAD_RESPONSE + GOOD_RESPONSE + 0.01 end),0),4) BAD_GOOD_RESPONSE_3M_PCT
       , round(nvl(sum(case when months_between(run_date, f.contact_month) <=6 then BAD_RESPONSE end)/
                  sum(case when months_between(run_date, f.contact_month) <=6 then BAD_RESPONSE + GOOD_RESPONSE + 0.01 end),0),4) BAD_GOOD_RESPONSE_6M_PCT
       , round(nvl(sum(case when months_between(run_date, f.contact_month) <=9 then BAD_RESPONSE end)/
                  sum(case when months_between(run_date, f.contact_month) <=9 then BAD_RESPONSE + GOOD_RESPONSE + 0.01 end),0),4) BAD_GOOD_RESPONSE_9M_PCT
       , round(nvl(sum(case when months_between(run_date, f.contact_month) <=12 then BAD_RESPONSE end)/
                  sum(case when months_between(run_date, f.contact_month) <=12 then BAD_RESPONSE + GOOD_RESPONSE + 0.01 end),0),4) BAD_GOOD_RESPONSE_12M_PCT
       , round(nvl(sum(case when months_between(run_date, f.contact_month) <=24 then BAD_RESPONSE end)/
                  sum(case when months_between(run_date, f.contact_month) <=24 then BAD_RESPONSE + GOOD_RESPONSE + 0.01 end),0),4) BAD_GOOD_RESPONSE_24M_PCT

       , round(nvl(sum(BAD_RESPONSE)/sum(ATTEMPT_CNT + 0.01),0),4) BAD_ATTEMPT_RESPONSE_CNT
       , round(nvl(sum(case when months_between(run_date, f.contact_month) <=3 then BAD_RESPONSE end)/
                  sum(case when months_between(run_date, f.contact_month) <=3 then ATTEMPT_CNT + 0.01 end),0),4) BAD_ATTEMPT_RESPONSE_3M_PCT
       , round(nvl(sum(case when months_between(run_date, f.contact_month) <=6 then BAD_RESPONSE end)/
                  sum(case when months_between(run_date, f.contact_month) <=6 then ATTEMPT_CNT + 0.01 end),0),4) BAD_ATTEMPT_RESPONSE_6M_PCT
       , round(nvl(sum(case when months_between(run_date, f.contact_month) <=9 then BAD_RESPONSE end)/
                  sum(case when months_between(run_date, f.contact_month) <=9 then ATTEMPT_CNT + 0.01 end),0),4) BAD_ATTEMPT_RESPONSE_9M_PCT
       , round(nvl(sum(case when months_between(run_date, f.contact_month) <=12 then BAD_RESPONSE end)/
                  sum(case when months_between(run_date, f.contact_month) <=12 then ATTEMPT_CNT + 0.01 end),0),4) BAD_ATTEMPT_RESPONSE_12M_PCT
       , round(nvl(sum(case when months_between(run_date, f.contact_month) <=24 then BAD_RESPONSE end)/
                  sum(case when months_between(run_date, f.contact_month) <=24 then ATTEMPT_CNT + 0.01 end),0),4) BAD_ATTEMPT_RESPONSE_24M_PCT

       , round(nvl(sum(BAD_RESPONSE)/sum(CONNECT_CNT + 0.01),0),4) BAD_CONNECT_RESPONSE_CNT
       , round(nvl(sum(case when months_between(run_date, f.contact_month) <=3 then BAD_RESPONSE end)/
                  sum(case when months_between(run_date, f.contact_month) <=3 then CONNECT_CNT + 0.01 end),0),4) BAD_CONNECT_RESPONSE_3M_PCT
       , round(nvl(sum(case when months_between(run_date, f.contact_month) <=6 then BAD_RESPONSE end)/
                  sum(case when months_between(run_date, f.contact_month) <=6 then CONNECT_CNT + 0.01 end),0),4) BAD_CONNECT_RESPONSE_6M_PCT
       , round(nvl(sum(case when months_between(run_date, f.contact_month) <=9 then BAD_RESPONSE end)/
                  sum(case when months_between(run_date, f.contact_month) <=9 then CONNECT_CNT + 0.01 end),0),4) BAD_CONNECT_RESPONSE_9M_PCT
       , round(nvl(sum(case when months_between(run_date, f.contact_month) <=12 then BAD_RESPONSE end)/
                  sum(case when months_between(run_date, f.contact_month) <=12 then CONNECT_CNT + 0.01 end),0),4) BAD_CONNECT_RESPONSE_12M_PCT
       , round(nvl(sum(case when months_between(run_date, f.contact_month) <=24 then BAD_RESPONSE end)/
                  sum(case when months_between(run_date, f.contact_month) <=24 then CONNECT_CNT + 0.01 end),0),4) BAD_CONNECT_RESPONSE_24M_PCT

       , round(nvl(sum(BAD_RESPONSE)/sum(CONTACT_CNT + 0.01),0),4) BAD_CONTACT_RESPONSE_CNT
       , round(nvl(sum(case when months_between(run_date, f.contact_month) <=3 then BAD_RESPONSE end)/
                  sum(case when months_between(run_date, f.contact_month) <=3 then CONTACT_CNT + 0.01 end),0),4) BAD_CONTACT_RESPONSE_3M_PCT
       , round(nvl(sum(case when months_between(run_date, f.contact_month) <=6 then BAD_RESPONSE end)/
                  sum(case when months_between(run_date, f.contact_month) <=6 then CONTACT_CNT + 0.01 end),0),4) BAD_CONTACT_RESPONSE_6M_PCT
       , round(nvl(sum(case when months_between(run_date, f.contact_month) <=9 then BAD_RESPONSE end)/
                  sum(case when months_between(run_date, f.contact_month) <=9 then CONTACT_CNT + 0.01 end),0),4) BAD_CONTACT_RESPONSE_9M_PCT
       , round(nvl(sum(case when months_between(run_date, f.contact_month) <=12 then BAD_RESPONSE end)/
                  sum(case when months_between(run_date, f.contact_month) <=12 then CONTACT_CNT + 0.01 end),0),4) BAD_CONTACT_RESPONSE_12M_PCT
       , round(nvl(sum(case when months_between(run_date, f.contact_month) <=24 then BAD_RESPONSE end)/
                  sum(case when months_between(run_date, f.contact_month) <=24 then CONTACT_CNT + 0.01 end),0),4) BAD_CONTACT_RESPONSE_24M_PCT

       , round(nvl(sum(CONNECT_CNT)/sum(ATTEMPT_CNT  + 0.01),0),4) CONNECT_RATE
       , round(nvl(sum(case when months_between(run_date, f.contact_month) <=3 then CONNECT_CNT end)/
                  sum(case when months_between(run_date, f.contact_month) <=3 then ATTEMPT_CNT  + 0.01 end),0),4) CONNECT_RATE_3M
       , round(nvl(sum(case when months_between(run_date, f.contact_month) <=6 then CONNECT_CNT end)/
                  sum(case when months_between(run_date, f.contact_month) <=6 then ATTEMPT_CNT  + 0.01 end),0),4) CONNECT_RATE_6M
       , round(nvl(sum(case when months_between(run_date, f.contact_month) <=9 then CONNECT_CNT end)/
                  sum(case when months_between(run_date, f.contact_month) <=9 then ATTEMPT_CNT  + 0.01 end),0),4) CONNECT_RATE_9M
       , round(nvl(sum(case when months_between(run_date, f.contact_month) <=12 then CONNECT_CNT end)/
                  sum(case when months_between(run_date, f.contact_month) <=12 then ATTEMPT_CNT  + 0.01 end),0),4) CONNECT_RATE_12M
       , round(nvl(sum(case when months_between(run_date, f.contact_month) <=24 then CONNECT_CNT end)/
                  sum(case when months_between(run_date, f.contact_month) <=24 then ATTEMPT_CNT  + 0.01 end),0),4) CONNECT_RATE_24M

       , round(nvl(sum(CONTACT_CLIENT_CNT)/sum(ATTEMPT_CNT + 0.01),0),4) CONTACT_CLIENT_RATE
       , round(nvl(sum(case when months_between(run_date, f.contact_month) <=3 then CONTACT_CLIENT_CNT end)/
                  sum(case when months_between(run_date, f.contact_month) <=3 then ATTEMPT_CNT + 0.01 end),0),4) CONTACT_CLIENT_RATE_3M
       , round(nvl(sum(case when months_between(run_date, f.contact_month) <=6 then CONTACT_CLIENT_CNT end)/
                  sum(case when months_between(run_date, f.contact_month) <=6 then ATTEMPT_CNT + 0.01 end),0),4) CONTACT_CLIENT_RATE_6M
       , round(nvl(sum(case when months_between(run_date, f.contact_month) <=9 then CONTACT_CLIENT_CNT end)/
                  sum(case when months_between(run_date, f.contact_month) <=9 then ATTEMPT_CNT + 0.01 end),0),4) CONTACT_CLIENT_RATE_9M
       , round(nvl(sum(case when months_between(run_date, f.contact_month) <=12 then CONTACT_CLIENT_CNT end)/
                  sum(case when months_between(run_date, f.contact_month) <=12 then ATTEMPT_CNT + 0.01 end),0),4) CONTACT_CLIENT_RATE_12M
       , round(nvl(sum(case when months_between(run_date, f.contact_month) <=24 then CONTACT_CLIENT_CNT end)/
                  sum(case when months_between(run_date, f.contact_month) <=24 then ATTEMPT_CNT + 0.01 end),0),4) CONTACT_CLIENT_RATE_24M

from pol_tru_crc_main_daily_1   a
--left join pol_tbl_tuan_previous_app p on a.agreementno = p.agreement_no
left join pol_tbl_raw_follow_data f on f.app_id = a.appl_id and trunc(a.run_date,''mm'') > trunc(f.contact_month,''mm'')
group by a.appl_id, a.run_date';

insert into pol_tbl_predue_crc_job_control_test
(select 'pol_predue_crc_main_daily_col',sysdate,'Ready' from dual);
commit;

--------------------utilization----------------------

drop_table_if_exists('pol_predue_crc_main_daily_utilization');
execute immediate 'create table pol_predue_crc_main_daily_utilization as
select /*+PARALLEL(+32)*/a.account_number  ,a.appl_id,run_date, due_date, due_dt,
balance_amt/credit_limit_amt utilization_percent,credit_limit_amt,  (credit_limit_amt-balance_amt) utilization
from pol_tru_crc_main_daily_1 a
left   join sdm.Sdm_Com_Crc_Payment_In_Stmt b  on a.account_number = b.account_no and trunc(b.due_dt,''mm'') = trunc(add_months(a.due_date,-1),''mm'')';


insert into pol_tbl_predue_crc_job_control_test
(select 'pol_predue_crc_main_daily_utilization',sysdate,'Ready' from dual);
commit;


drop_table_if_exists('pol_predue_crc_main_daily_maxdpd') ;
execute immediate 'create table pol_predue_crc_main_daily_maxdpd as
select /*+PARALLEL(+32)*/appl_id, run_date, max (t.dpd) max_dpd
from pol_tru_crc_main_daily_1 a
left join  pol_tbl_raw_beh_data t on a.appl_id = t.app_id_c
where trunc(to_date(report_month,''yyyy/mm''),''mm'') < trunc(run_date,''mm'')
group by appl_id , run_date';

insert into pol_tbl_predue_crc_job_control_test
(select 'pol_predue_crc_main_daily_maxdpd',sysdate,'Ready' from dual);
commit;



----------------------- pcb var-----------------------------------------------


drop_table_if_exists('pol_predue_crc_main_daily_instprof');
execute immediate 'create table pol_predue_crc_main_daily_instprof as
select /*+ parallel(32)*/ * from pol_view_pcbscrub_inst_profile
where id_no  in (select id_no from pol_tru_crc_main_daily_1)' ;

--drop table pol_tbl_csa_crc_val_instprof2_new_model ;
drop_table_if_exists('pol_predue_crc_main_daily_instprof2');
execute immediate 'create table pol_predue_crc_main_daily_instprof2 as
with tbl as(
select /*+ parallel(32)*/id_no, max(to_char(datecreated,''yyyy/mm'')) date_max
from pol_predue_crc_main_daily_instprof s
where cbcontractcode is not null and referenceyearmonth is not null
group by id_no)
select s.date_max, t.* from tbl s
left join pol_predue_crc_main_daily_instprof t on s.id_no = t.id_no and s.date_max = to_char(t.datecreated,''yyyy/mm'')
where t.id_no IS NOT NULL';

--select count (*) from pol_tru_crc_pre_new_model_pcb
--drop table pol_tru_crc_pre_new_model_pcb;
drop_table_if_exists('pol_predue_crc_main_daily_pcb');
execute immediate 'create table pol_predue_crc_main_daily_pcb as
select /*+PARALLEL(+32)*/appl_id,run_date,
max (min_m_default) max_m_default,
min (min_m_default) min_m_default
from (
select/*+PARALLEL(+32)*/ a.appl_id,run_date,
(case when default_ > 0 then months_between(trunc(a.due_date,''mm''),trunc(to_date(referenceyearmonth,''yyyymm''),''mm'')) else 0 end ) min_m_default
from pol_tru_crc_main_daily_1 a
left join pol_predue_crc_main_daily_instprof2 t on a.id_no = t.id_no
and trunc(to_date(referenceyearmonth,''yyyymm''),''mm'') < trunc(a.run_date,''mm'')  )
group by appl_id,run_date';


--drop table pol_tru_crc_pre_varcheck_new1_m_default ;
drop_table_if_exists('pol_predue_crc_main_daily_m_default');
execute immediate 'create table pol_predue_crc_main_daily_m_default as
select /*+ parallel(32)*/ a.id_no, run_date, max(default_) m_default from
pol_tru_crc_main_daily_1 a
left join pol_predue_crc_main_daily_instprof2 t on a.id_no = t.id_no
where to_date(referenceyearmonth,''yyyymm'') < trunc(a.run_date,''mm'')
group by a.id_no,run_date';



/*select * from (
select \*+PARALLEL(+32)*\b.* from
(select * from pol_tru_crc_pre_new_model_cli_raw1
where cnt_row1_des = 1 and without_due_flag = 1) a
left   join sdm.sdm_col_account_crc t on a.appl_id = t.appl_id
left   join sdm.Sdm_Com_Crc_Payment_In_Stmt b  on t.account_number = b.account_no and a.due_dt = b.due_dt
and payment_without_due_dt > run_date where b.account_no is not null

select \*+PARALLEL(+32)*\max(payment_without_due_dt-due_dt) from sdm.Sdm_Com_Crc_Payment_In_Stmt
where trunc(due_dt,'mm') = trunc(to_date('08/2019','mm/yyyy'),'mm')
*/

----------------- cli -  prior due - utilization raw-------------------------------

drop_table_if_exists('pol_predue_crc_main_daily_raw1') ;
execute immediate 'create table pol_predue_crc_main_daily_raw1 as
select /*+PARALLEL(+32)*/a.appl_id, due_dt,run_date,
 lag((payment_within_due_amt-b.min_due_amt),1) over ( partition  by a.appl_id,a.run_date order by b.due_dt asc) as min_incre_lag,
 lag((payment_within_due_amt/case when
 b.min_due_amt =0 or b.min_due_amt is null then -1 else b.min_due_amt end ),1) over ( partition  by a.appl_id,a.run_date order by b.due_dt asc) as min_incre_percent,

 lag((payment_within_due_amt-PMT_INTR_FREE),1) over ( partition  by a.appl_id,a.run_date order by b.due_dt asc) as full_due_incre_lag,
 lag((payment_within_due_amt/case when
 PMT_INTR_FREE = 0 or PMT_INTR_FREE is null then -1 else PMT_INTR_FREE end ),1) over ( partition  by a.appl_id,run_date order by b.due_dt asc) as full_due_incre_percent,
 lag((payment_within_due_dt-due_dt),1) over ( partition  by a.appl_id,a.run_date order by b.due_dt asc) as prior_due_lag,
 lag((payment_within_due_dt-statement_dt),1) over ( partition  by a.appl_id,a.run_date order by b.due_dt asc) as prior_statement_lag,
 lag((payment_within_cycle_dt-due_dt),1) over ( partition  by a.appl_id,a.run_date order by b.due_dt asc) as prior_cycle_lag,
 lag((payment_within_cycle_amt/case when b.min_due_amt =0 or b.min_due_amt is null then -1 else b.min_due_amt end ),1) over ( partition  by a.appl_id,a.run_date order by b.due_dt asc) as min_incre_percent_cycle,
 lag((payment_without_due_dt-due_dt),1) over ( partition  by a.appl_id,run_date order by b.due_dt asc) as pmt_without_lag,
 row_number() over (partition by a.appl_id,run_date order by b.due_dt desc) as cnt_row1_des,
 row_number() over (partition by a.appl_id,run_date order by b.due_dt asc) as cnt_row2_acs,
 case when payment_within_due_amt is not null then 1 else 0 end as within_due_flag,
  case when payment_without_due_amt is not null then 1 else 0 end as without_due_flag
from pol_tru_crc_main_daily_1   a
--left   join sdm.sdm_col_account_crc t on a.appl_id = t.appl_id
left   join sdm.Sdm_Com_Crc_Payment_In_Stmt b  on a.account_number = b.account_no and due_dt between add_months(a.run_date,-10) and a.run_date
left   join sdm.sdm_com_crc_stmt x on a.account_number = x.account_no and PMT_INTR_FREE> 0 and x.date_curr_stmt = b.statement_dt';

insert into pol_tbl_predue_crc_job_control_test
(select 'pol_predue_crc_main_daily_raw1',sysdate,'Ready' from dual);
commit;

---------------MIN DUE INCREASE PRIOR DUE STATEMENT AMT------------

--drop table pol_tru_crc_pre_new_model_cli_raw_final1 ;
drop_table_if_exists('pol_predue_crc_main_daily_cli_raw_final1');
execute immediate 'create table pol_predue_crc_main_daily_cli_raw_final1 as
select a.appl_id , run_date,
sum(case when   cnt_row1_des<=6 and cnt_row1_des>3 then min_incre_lag else 0 end) as min_incre_last_3_6months,
sum(case when  cnt_row1_des<=3 then min_incre_lag else 0 end) as min_incre_last_3months,
max(case when due_dt is null then -1 when cnt_row1_des<=6 and cnt_row1_des>3 then min_incre_lag else -1 end) as m_min_incre_last_3_6months,
max(case when due_dt is null then -1 when cnt_row1_des<=3 then min_incre_lag else -1 end) as m_min_incre_last_3months,
max(case when due_dt is null then -1 when cnt_row1_des = 2 then min_incre_lag else -1 end) as min_incre_prev_month,
max(case when due_dt is null then -1 when cnt_row1_des = 1 then min_incre_lag else -1 end) as min_incre_last_month,



sum(case when   cnt_row1_des<=6 and cnt_row1_des>3 then min_incre_percent else 0 end) as min_incre_percent_3_6months,
sum(case when  cnt_row1_des<=3 then min_incre_percent else 0 end) as min_incre_percent_3months,
max(case when due_dt is null then -1 when cnt_row1_des<=6 and cnt_row1_des>3 then min_incre_percent else -1 end) as m_min_incre_percent_3_6months,
max(case when due_dt is null then -1 when cnt_row1_des<=3 then min_incre_percent else -1 end) as mmin_incre_percent_3months,
max(case when due_dt is null then -1 when cnt_row1_des = 2 then min_incre_percent else -1 end) as min_incre_percent_prev_month,
max(case when due_dt is null then -1 when cnt_row1_des = 1 then min_incre_percent else -1 end) as min_incre_percent_last_month,


sum(case when   cnt_row1_des<=6 and cnt_row1_des>3 then full_due_incre_percent else 0 end) as full_due_incre_percent_3_6months,
sum(case when  cnt_row1_des<=3 then full_due_incre_percent else 0 end) as full_due_incre_percent_last_3months,
max(case when due_dt is null then -1 when cnt_row1_des<=6 and cnt_row1_des>3 then full_due_incre_percent else -1 end) as m_full_due_incre_percent_3_6months,
max(case when due_dt is null then -1 when cnt_row1_des<=3 then full_due_incre_percent else -1 end) as full_due_incre_percent_3months,
max(case when due_dt is null then -1 when cnt_row1_des = 2 then full_due_incre_percent else -1 end) as full_due_incre_percent_prev_month,
max(case when due_dt is null then -1 when cnt_row1_des = 1 then full_due_incre_percent else -1 end) as full_due_incre_percent_last_month,



sum(case when   cnt_row1_des<=6 and cnt_row1_des>3 then min_incre_percent_cycle else 0 end) as min_incre_percent_cycle_3_6months,
sum(case when  cnt_row1_des<=3 then min_incre_percent_cycle else 0 end) as min_incre_percent_cycle_3months,
max(case when due_dt is null then -1 when cnt_row1_des<=6 and cnt_row1_des>3 then min_incre_percent_cycle else -1 end) as m_min_incre_percent_cycle_3_6months,
max(case when due_dt is null then -1 when cnt_row1_des<=3 then min_incre_percent_cycle else -1 end) as m_min_incre_percent_cycle_3months,
max(case when due_dt is null then -1 when cnt_row1_des = 2 then min_incre_percent_cycle else -1 end) as min_incre_percent_cycle_prev_month,
max(case when due_dt is null then -1 when cnt_row1_des = 1 then min_incre_percent_cycle else -1 end) as min_incre_percent_cycle_last_month,



sum(case when  cnt_row1_des<=6 and cnt_row1_des>3 then prior_due_lag else 0 end) as prior_due_last_3_6months,
sum(case when due_dt is null then -1 when cnt_row1_des<=3 then prior_due_lag else 0 end) as prior_due_last_3months,
min(case when due_dt is null then -1 when cnt_row1_des<=6 and cnt_row1_des>3 then prior_due_lag else null end) as m_prior_due_last_3_6months,
min(case when due_dt is null then -1 when cnt_row1_des<=3 then prior_due_lag else null end) as m_prior_due_last_3months,
min(case when due_dt is null then -1 when cnt_row1_des = 2 then prior_due_lag else null end) as prior_due_prev_month,
min(case when due_dt is null then -1 when cnt_row1_des = 1 then prior_due_lag else null end) as prior_due_last_month,


sum(case when  cnt_row1_des<=6 and cnt_row1_des>3 then prior_cycle_lag else 0 end) as prior_cycle_3_6months,
sum(case when due_dt is null then -1 when cnt_row1_des<=3 then prior_cycle_lag else 0 end) as prior_cycle_3months,
min(case when due_dt is null then -1 when cnt_row1_des<=6 and cnt_row1_des>3 then prior_due_lag else null end) as m_prior_cycle_3_6months,
min(case when due_dt is null then -1 when cnt_row1_des<=3 then prior_cycle_lag else null end) as m_prior_cycle_last_3months,
min(case when due_dt is null then -1 when cnt_row1_des = 2 then prior_cycle_lag else null end) as prior_cycle_prev_month,
min(case when due_dt is null then -1 when cnt_row1_des = 1 then prior_cycle_lag else null end) as prior_cycle_last_month,


sum(case when  cnt_row1_des<=6 and cnt_row1_des>3 then pmt_without_lag else 0 end) as pmt_without_3_6months,
sum(case when due_dt is null then -1 when cnt_row1_des<=3 then pmt_without_lag else 0 end) as pmt_without_3months,
min(case when due_dt is null then -1 when cnt_row1_des<=6 and cnt_row1_des>3 then pmt_without_lag else null end) as m_pmt_without_3_6months,
min(case when due_dt is null then -1 when cnt_row1_des<=3 then pmt_without_lag else null end) as m_pmt_without_last_3months,
min(case when due_dt is null then -1 when cnt_row1_des = 2 then pmt_without_lag else null end) as pmt_without_prev_month,
min(case when due_dt is null then -1 when cnt_row1_des = 1 then pmt_without_lag else null end) as pmt_without_last_month,



sum(case when  cnt_row1_des<=6 and cnt_row1_des>3 then prior_statement_lag else 0 end) as prior_statement_last_3_6months,
sum(case when due_dt is null then -1 when cnt_row1_des<=3 then prior_statement_lag else 0 end) as prior_statement_last_3months,
min(case when due_dt is null then -1 when cnt_row1_des<=6 and cnt_row1_des>3 then prior_statement_lag else -1 end) as m_prior_statement_last_3_6months,
min(case when due_dt is null then -1 when cnt_row1_des<=3 then prior_statement_lag else null end) as m_prior_statement_last_3months,
min(case when due_dt is null then -1 when cnt_row1_des = 1 then prior_statement_lag else null end) as prior_statement_last_month,
min(case when due_dt is null then -1 when cnt_row1_des = 2 then prior_statement_lag else null end) as prior_statement_prev_month,
sum(without_due_flag) without_due_flag, sum(within_due_flag) within_due_flag
from pol_predue_crc_main_daily_raw1 a
group by a.appl_id,run_date' ;

insert into pol_tbl_predue_crc_job_control_test
(select 'pol_predue_crc_main_daily_cli_raw_final1',sysdate,'Ready' from dual);
commit;
--------------------------------avg spending------------------



--drop table pol_tru_crc_pre_new_model_spend_3m ;
drop_table_if_exists('pol_predue_crc_main_daily_spend_3m');
Execute immediate 'create table pol_predue_crc_main_daily_spend_3m as
select /*+PARALLEL(+64)*/a.appl_id ,run_date, avg(amount) avg_retail
from pol_tru_crc_main_daily_1 a
left join sdm.sdm_col_transaction_crc t on a.appl_id = t.appl_id
--left join mis.MV_CRC_TRANS_CD x on a.appl_id = x.appl_id
where trunc(t.effect_date,''mm'') between add_months(a.run_date,-3) and  run_date
and transaction_code in (select transaction_code from cicdata.MV_CRC_TRANS_CD
  where type IN (''RETAIL''))
group by a.appl_id,run_date' ;


--drop table pol_tru_crc_pre_new_model_spend_6m;
drop_table_if_exists('pol_predue_crc_main_daily_spend_6m');
Execute immediate 'create table pol_predue_crc_main_daily_spend_6m  as
select /*+PARALLEL(+64)*/a.appl_id ,run_date, avg(amount)avg_retail
from pol_tru_crc_main_daily_1 a
left join sdm.sdm_col_transaction_crc t on a.appl_id = t.appl_id
--left join mis.MV_CRC_TRANS_CD x on a.appl_id = x.appl_id
where trunc(t.effect_date,''mm'') between add_months(a.run_date,-6) and  run_date
and transaction_code in (select transaction_code from cicdata.MV_CRC_TRANS_CD
  where type IN (''RETAIL''))
group by a.appl_id,run_date' ;

--drop table  pol_predue_crc_main_daily_spend_12m ;
drop_table_if_exists('pol_predue_crc_main_daily_spend_12m');
Execute immediate 'create table pol_predue_crc_main_daily_spend_12m as
select /*+PARALLEL(+64)*/a.appl_id ,run_date, avg(amount)avg_retail
from pol_tru_crc_main_daily_1 a
left join sdm.sdm_col_transaction_crc t on a.appl_id = t.appl_id
--left join mis.MV_CRC_TRANS_CD x on a.appl_id = x.appl_id
where trunc(t.effect_date,''mm'') between add_months(a.run_date,-12) and  run_date
and transaction_code in (select transaction_code from cicdata.MV_CRC_TRANS_CD
  where type IN (''RETAIL''))
group by a.appl_id,run_date';

drop_table_if_exists('pol_predue_crc_main_daily_spend');
execute immediate 'create table pol_predue_crc_main_daily_spend as
select/*+PARALLEL(+64)*/ a.appl_id,a.run_date, t.avg_retail avg_retail_3m, e.avg_retail avg_retail_6m, c.avg_retail avg_retail_12m ,
(t.avg_retail/credit_limit_amt) spending_cre_lim_3m, (e.avg_retail/credit_limit_amt) spending_cre_lim_6m,(c.avg_retail/credit_limit_amt) spending_cre_lim_12m
from pol_tru_crc_main_daily_1 a
left join pol_predue_crc_main_daily_utilization x on a.appl_id = x.appl_id and  a.run_date = x.run_date
left join pol_predue_crc_main_daily_spend_3m t on a.appl_id = t.appl_id and a.run_date = t.run_date
left join pol_predue_crc_main_daily_spend_6m e on a.appl_id = e.appl_id and a.run_date = e.run_date
left join pol_predue_crc_main_daily_spend_12m c on a.appl_id = c.appl_id and a.run_date = c.run_date';


insert into pol_tbl_predue_crc_job_control_test
(select 'pol_predue_crc_main_daily_spend',sysdate,'Ready' from dual);
commit;

----------------------------- cli flag and cnt---------------------------------------


--drop table pol_tru_crc_new_model_cli_list;
drop_table_if_exists('pol_predue_crc_main_daily_cli_list');
execute immediate 'create table pol_predue_crc_main_daily_cli_list as
select a.appl_id,
row_number()  over (partition by a.appl_id,a.run_date order by increased_date desc ) cnt_des, a.run_date, increased_crlimit, crlimit, increased_date
from  pol_tru_crc_main_daily_1 a
left join  common.pol_tbl_cli_list t on a.appl_id = t.appl_id
where increased_date < a.run_date' ;




--drop table pol_tru_crc_new_model_cli_flag ;
drop_table_if_exists('pol_predue_crc_main_daily_cli_flag');
execute immediate 'create table pol_predue_crc_main_daily_cli_flag as
select a.*,(increased_crlimit-crlimit) cli_amt, case when t.appl_id is not null then 1 else 0 end as cli_flag
 from pol_tru_crc_main_daily_1 a
left join (select a.* from pol_predue_crc_main_daily_cli_list a
where cnt_des = 1 ) t on a.appl_id = t.appl_id and a.run_date = t.run_date';



drop_table_if_exists('pol_predue_crc_main_daily_cli_cnt');
execute immediate 'create table pol_predue_crc_main_daily_cli_cnt as
select a.*, case when t.appl_id is not null then cli_cnt else 0 end as cli_cnt
from pol_tru_crc_main_daily_1 a
left join (select appl_id, run_date, count(appl_id) cli_cnt from pol_tru_crc_new_model_cli_list a
group by appl_id,run_date ) t on a.appl_id = t.appl_id and a.run_date = t.run_date';


--drop table pol_predue_crc_main_daily_list_target ;
drop_table_if_exists('pol_predue_crc_main_daily_cli_list_final');
execute immediate 'create table pol_predue_crc_main_daily_cli_list_final as
select a.appl_id, a.run_date,  cli_flag,cli_amt, cli_cnt from pol_tru_crc_main_daily_1 a
left join  pol_predue_crc_main_daily_cli_cnt t on a.appl_id = t.appl_id and a.run_date = t.run_date
left join pol_predue_crc_main_daily_cli_flag x on a.appl_id = x.appl_id and a.run_date = x.run_date';


insert into pol_tbl_predue_crc_job_control_test
(select 'pol_predue_crc_main_daily_cli_list_final',sysdate,'Ready' from dual);
commit;
-------------------INCOME VS SAVING VAR-----------------------------------------------

--select * from pol_predue_crc_main_daily_newvar
drop_table_if_exists('pol_predue_crc_main_daily_newvar');
execute immediate 'create table pol_predue_crc_main_daily_newvar as
select a.appl_id,a.run_date,
real_income,
--target,
saving,
S_IPP_ACT_AMT_9M/case when saving = 0 or saving is null then -1 else saving end as s_ipp_amt_saving9m,
S_CASH_ACT_AMT_9M/case when saving = 0 or saving is null then -1 else saving end as s_cash_amt_saving9m,
S_IPP_ACT_AMT_6M/case when saving = 0 or saving is null then -1 else saving end as s_ipp_amt_saving6m,
S_CASH_ACT_AMT_6M/case when saving = 0 or saving is null then -1 else saving end as s_cash_amt_saving6m,
S_IPP_ACT_AMT_3M/case when saving = 0 or saving is null then -1 else saving end as s_ipp_amt_saving3m,
S_CASH_ACT_AMT_3M/case when saving = 0 or saving is null then -1 else saving end as s_cash_amt_saving3m,
M_IPP_ACT_AMT_9M/case when saving = 0 or saving is null then -1 else saving end as m_ipp_amt_saving9m,
M_CASH_ACT_AMT_9M/case when saving = 0 or saving is null then -1 else saving end as m_cash_amt_saving9m,
M_IPP_ACT_AMT_6M/case when saving = 0 or saving is null then -1 else saving end as m_ipp_amt_saving6m,
M_CASH_ACT_AMT_6M/case when saving = 0 or saving is null then -1 else saving end as m_cash_amt_saving6m,
M_IPP_ACT_AMT_3M/case when saving = 0 or saving is null then -1 else saving end as m_ipp_amt_saving3m,
M_CASH_ACT_AMT_3M/case when saving = 0 or saving is null then -1 else saving end as m_cash_amt_saving3m,
AVG_RETAIL_3M/case when saving = 0 or saving is null then -1 else saving end as  avg_retail_saving3m,
AVG_RETAIL_6M  /case when saving = 0 or saving is null then -1 else saving end as avg_retail_saving6m,
AVG_RETAIL_12M/case when saving = 0 or saving is null then -1 else saving end as avg_retail_saving12m,
(credit_limit_amt/case when real_income = 0 or real_income is null then -1 else real_income end )as crlim_income,
(credit_limit_amt/case when saving = 0 or saving is null then -1 else saving end ) as crlim_saving,
personal_expense
from pol_tru_crc_main_daily_1  a
left join pol_predue_crc_main_daily_cash_act_amt t on a.appl_id = t.appl_id and a.run_date = t.run_date
left join pol_predue_crc_main_daily_utilization t1 on a.appl_id = t1.appl_id and a.run_date = t1.run_date
LEFT join pol_predue_crc_main_daily_spend  a1 on a.appl_id = a1.appl_id and a.run_date = a1.run_date';

insert into pol_tbl_predue_crc_job_control_test
(select 'pol_predue_crc_main_daily_newvar',sysdate,'Ready' from dual);
commit;


--drop table pol_tru_crc_pre_new_model_beh_raw  ;
drop_table_if_exists('pol_tru_crc_main_daily_beh_raw');
execute immediate ' create table pol_tru_crc_main_daily_beh_raw as
select /*+ USE_HASH(a,b) parallel(32) */a.appl_id,run_date, b.*,
row_number() over (partition by a.appl_id,run_date order by b.report_month desc) as cnt_row1,
row_number() over (partition by a.appl_id,run_date order by b.report_month asc) as cnt_row2,
lag(dpdall,1) over (partition by a.appl_id,run_date order by b.report_month asc) as dpdall_lag,
lag(cfall,1) over (partition by a.appl_id,run_date order by b.report_month asc) as cfall_lag,
lag(enrall,1) over (partition by a.appl_id ,run_date order by b.report_month asc) as enrall_lag,
lag(numactl,1) over (partition by a.appl_id,run_date order by b.report_month asc) as numactl_lag,
lag(numcll,1) over (partition by a.appl_id,run_date order by b.report_month asc) as numcll_lag
from pol_tru_crc_main_daily_1  a
left join risk_nhutltm.pol_tbl_raw_beh_data b on a.appl_id=b.app_id_c
and trunc(a.run_date,''mm'') > trunc(to_date(report_month,''yyyy/mm''),''mm'') and enrall>0';


--drop table pol_tru_crc_pre_new_model_raw_beh;
drop_table_if_exists('pol_tru_crc_main_daily_raw_beh');
execute immediate 'create table pol_tru_crc_main_daily_raw_beh /*+ append parallel(32) */ as
select /*+ USE_HASH(s,r,t) parallel(32) */ a.appl_id ,run_date,
max(early_pmt_day_max) as max_early_pmt_pay_max,
min(early_pmt_day_max) as min_early_pmt_pay_max,
min(early_pmt_day_min) as min_early_pmt_pay_min,
max(early_pmt_day_min) as max_early_pmt_pay_min,
max(cnt_row1) as max_life_time,
max(case when report_month is null then -1 else dpdall end) as max_dpd_ever,
max(case when report_month is null then -1 when cnt_row1<=3 then dpdall else -1 end) as max_dpd_last_3_active_months,
max(case when report_month is null then -1 when cnt_row1<=6 and cnt_row1>3 then dpdall else -1 end) as max_dpd_last_3_6_active_months,
max(case when report_month is null then -1 when cnt_row1<=12 and cnt_row1>6 then dpdall else -1 end) as max_dpd_last_6_12_active_months,
max(case when report_month is null then -1 when cnt_row1<=24 and cnt_row1>12 then dpdall else -1 end) as max_dpd_last_12_24_active_months,
max(case when report_month is null then -1 else months_between(run_date,to_date(report_month,''yyyy/mm'')) end) as max_months_journey,
min(case when report_month is null then -1 else months_between(run_date,to_date(report_month,''yyyy/mm'')) end) as min_months_journey,
max(months_between(run_date,case when DPDall>0 then to_date(report_month,''yyyy/mm'') else to_date(''01Jan1900'') end)) as max_months_DPD_0,
min(months_between(run_date,case when DPDall>0 then to_date(report_month,''yyyy/mm'') else to_date(''01Jan1900'') end)) as min_months_DPD_0,
sum(case when report_month is null then -1 when dpdall>0 then 1 else 0 end)/max(case when report_month is null then 1 else cnt_row2 end) as months_dpd_0_cus_journey,
sum(case when report_month is null then -1 when dpdall>10 then 1 else 0 end)/max(case when report_month is null then 1 else cnt_row2 end) as months_dpd_10_cus_journey,
sum(case when report_month is null then -1 when dpdall>30 then 1 else 0 end)/max(case when report_month is null then 1 else cnt_row2 end) as months_dpd_30_cus_journey,
avg(case when report_month is null then -1 when cnt_row1<=3 then enrall else null end) as avg_enr_all_last_3m,
avg(case when report_month is null then -1 when cnt_row1> 3 and cnt_row1<=6 then enrall else null end) as avg_enr_all_last_3_6m,
avg(case when report_month is null then -1 when cnt_row1> 6 and cnt_row1<=12 then enrall else null end) as avg_enr_all_last_6_12m,
avg(case when report_month is null then -1 when cnt_row1> 12 and cnt_row1<=24 then enrall else null end) as avg_enr_all_last_12_24m,
avg(case when report_month is null then -1 when cnt_row1<=3 then enrall else null end)/
max(case when report_month is null then 1 when cnt_row1<=3 then enrall else null end) as avg_enr_ratio_last_3m,
avg(case when report_month is null then -1 when cnt_row1> 3 and cnt_row1<=6 then enrall else null end)/
max(case when report_month is null then 1 when cnt_row1> 3 and cnt_row1<=6 then enrall else null end) as avg_enr_ratio_last_3_6m,
avg(case when report_month is null then -1 when cnt_row1> 6 and cnt_row1<=12 then enrall else null end)/
max(case when report_month is null then 1 when cnt_row1> 6 and cnt_row1<=12 then enrall else null end) as avg_enr_ratio_last_6_12m,
avg(case when report_month is null then -1 when cnt_row1> 12 and cnt_row1<=24 then enrall else null end)/
max(case when report_month is null then 1 when cnt_row1> 12 and cnt_row1<=24 then enrall else null end) as avg_enr_ratio_last_12_24m,
avg(case when report_month is null then -1 else enrall end)/
max(case when report_month is null then 1 else enrall end) as avg_enr_ratio_ever,
max(case when report_month is null then -1 else cfall end) as max_cf_all_ever,
max(case when report_month is null then -1 else amdall end) as max_amd_all_ever,
max(case when report_month is null then -1 when cnt_row1> 0 and cnt_row1<=3 then cfall else -1 end) as max_cf_all_last_3m,
max(case when report_month is null then -1 when cnt_row1> 3 and cnt_row1<=6 then cfall else -1 end) as max_cf_all_last_3_6m,
max(case when report_month is null then -1 when cnt_row1> 6 and cnt_row1<=12 then cfall else -1 end) as max_cf_all_last_6_12m,
max(case when report_month is null then -1 when cnt_row1> 12 and cnt_row1<=24 then cfall else -1 end) as max_cf_all_last_12_24m,
max((case when report_month is null then -1 else cfall end)/(case when report_month is null then 1 else greatest(amdall,1) end)) max_cf_amd_ever,
max((case when report_month is null then -1 when cnt_row1> 0 and cnt_row1<=3 then cfall else -1 end)/
(case when report_month is null then -1 when cnt_row1> 3 and cnt_row1<=6 then greatest(amdall,1) else 1 end)) as max_cf_amd_last_3m,
max((case when report_month is null then -1 when cnt_row1> 3 and cnt_row1<=6 then cfall else -1 end)/
(case when report_month is null then -1 when cnt_row1> 3 and cnt_row1<=6 then greatest(amdall,1) else 1 end)) as max_cf_amd_last_3_6m,
max((case when report_month is null then -1 when cnt_row1> 6 and cnt_row1<=12 then cfall else -1 end)/
(case when report_month is null then -1 when cnt_row1> 6 and cnt_row1<=12 then greatest(amdall,1) else 1 end)) as max_cf_amd_last_6_12m,
max((case when report_month is null then -1 when cnt_row1> 12 and cnt_row1<=24 then cfall else -1 end)/
(case when report_month is null then -1 when cnt_row1> 12 and cnt_row1<=24 then greatest(amdall,1) else 1 end)) as max_cf_amd_last_12_24m,
max(case when report_month is null then -1 else arrall end) as max_arr_ever,
max(case when report_month is null then -1 when cnt_row1> 0 and cnt_row1<=3 then arrall else -1 end) as max_arr_last_3m,
max(case when report_month is null then -1 when cnt_row1> 3 and cnt_row1<=6 then arrall else -1 end) as max_arr_last_3_6m,
max(case when report_month is null then -1 when cnt_row1> 6 and cnt_row1<=12 then arrall else -1 end) as max_arr_last_6_12m,
max(case when report_month is null then -1 when cnt_row1> 12 and cnt_row1<=24 then arrall else -1 end) as max_arr_last_12_24m,
max((case when report_month is null then -1 else nvl(arrall,-1) end)/(case when report_month is null then 1 else nvl(enrall,1) end)) as max_arr_enr_ever,
max((case when report_month is null then -1 when cnt_row1> 0 and cnt_row1<=3 then nvl(arrall,-1) else -1 end)/
(case when report_month is null then 1 when cnt_row1> 0 and cnt_row1<=3 then nvl(enrall,1) else 1 end)) as max_arr_enr_last_3m,
max((case when report_month is null then -1 when cnt_row1> 3 and cnt_row1<=6 then nvl(arrall,-1) else -1 end)/
(case when report_month is null then 1 when cnt_row1> 3 and cnt_row1<=6 then nvl(enrall,1) else 1 end)) as max_arr_enr_last_3_6m,
max((case when report_month is null then -1 when cnt_row1> 6 and cnt_row1<=12 then nvl(arrall,-1) else -1 end)/
(case when report_month is null then 1 when cnt_row1> 6 and cnt_row1<=12 then nvl(enrall,1) else 1 end)) as max_arr_enr_last_6_12m,
max((case when report_month is null then -1 when cnt_row1> 12 and cnt_row1<=24 then nvl(arrall,-1) else -1 end)/
(case when report_month is null then 1 when cnt_row1> 12 and cnt_row1<=24 then nvl(enrall,1) else 1 end)) as max_arr_enr_last_12_24m,
max(case when report_month is null then -1 else nvl(limall,-1) end) as max_lim_ever,
max(case when report_month is null then -1 when cnt_row1> 0 and cnt_row1<=3 then nvl(limall,-1) else -1 end) as max_lim_last_3m,
max(case when report_month is null then -1 when cnt_row1> 3 and cnt_row1<=6 then nvl(limall,-1) else -1 end) as max_lim_last_3_6m,
max(case when report_month is null then -1 when cnt_row1> 6 and cnt_row1<=12 then nvl(limall,-1) else -1 end) as max_lim_last_6_12m,
max(case when report_month is null then -1 when cnt_row1> 12 and cnt_row1<=24 then nvl(limall,-1) else -1 end) as max_lim_last_12_24m,
sum(case when cfall>amdall and amdall>0 then 1 else 0 end) as cf_amd_comp_ever,
sum(case when cnt_row1> 0 and cnt_row1<=3 and cfall>amdall and amdall>0 then 1 else 0 end) as cf_amd_comp_last_3m,
sum(case when cnt_row1> 3 and cnt_row1<=6 and cfall>amdall and amdall>0 then 1 else 0 end) as cf_amd_comp_last_3_6m,
sum(case when cnt_row1> 6 and cnt_row1<=12 and cfall>amdall and amdall>0 then 1 else 0 end) as cf_amd_comp_last_6_12m,
sum(case when cnt_row1> 12 and cnt_row1<=24 and cfall>amdall and amdall>0 then 1 else 0 end) as cf_amd_comp_last_12_24m,
max(case when report_month is null then 0 else nvl(numactl,0) end) as max_actl_ever,
max(case when cnt_row1> 0 and cnt_row1<=3 then nvl(numactl,0) else 0 end) as max_actl_last_3m,
max(case when cnt_row1> 3 and cnt_row1<=6 then nvl(numactl,0) else 0 end) as max_actl_last_3_6m,
max(case when cnt_row1> 6 and cnt_row1<=12 then nvl(numactl,0) else 0 end) as max_actl_last_6_12m,
max(case when cnt_row1> 12 and cnt_row1<=24 then nvl(numactl,0) else 0 end) as max_actl_last_12_24m,
max(case when report_month is null then 0 else nvl(numcll,0) end) as max_cll_ever,
max(case when cnt_row1> 0 and cnt_row1<=3 then nvl(numcll,0) else 0 end) as max_cll_last_3m,
max(case when cnt_row1> 3 and cnt_row1<=6 then nvl(numcll,0) else 0 end) as max_cll_last_3_6m,
max(case when cnt_row1> 6 and cnt_row1<=12 then nvl(numcll,0) else 0 end) as max_cll_last_6_12m,
max(case when cnt_row1> 12 and cnt_row1<=24 then nvl(numcll,0) else 0 end) as max_cll_last_12_24m,
max(case when report_month is null then 0 else nvl(numcll,0)+nvl(numactl,0) end) as max_alctcll_ever,
max(case when cnt_row1> 0 and cnt_row1<=3 then nvl(numcll,0)+nvl(numactl,0) else 0 end) as max_atclcll_last_3m,
max(case when cnt_row1> 3 and cnt_row1<=6 then nvl(numcll,0)+nvl(numactl,0) else 0 end) as max_atclcll_last_3_6m,
max(case when cnt_row1> 6 and cnt_row1<=12 then nvl(numcll,0)+nvl(numactl,0) else 0 end) as max_atclcll_last_6_12m,
max(case when cnt_row1> 12 and cnt_row1<=24 then nvl(numcll,0)+nvl(numactl,0) else 0 end) as max_atclcll_last_12_24m,
max(case when nvl(numactl_lag,0)<numactl then cnt_row1 else 0 end) as max_months_last_active,
max(case when nvl(numcll_lag,0)<numcll then cnt_row1 else 0 end) as max_months_last_close
from pol_tru_crc_main_daily_beh_raw  a
group by a.appl_id,run_date';


insert into pol_tbl_predue_crc_job_control_test
(select 'pol_tru_crc_main_daily_raw_beh',sysdate,'Ready' from dual);
commit;

--truncate table pol_tru_crc_main_daily_final_var
insert into pol_tru_crc_main_daily_final_var_test(
--create table pol_tru_crc_main_daily_final_var_test as
select /*+PARALLEL(+64)*/ a.appl_id, a.run_date,
--case when a.dpd1 <= 3 then 0 else 1 end as target,
MAX_DPD,ATTEMPT_3M_CNT,MAX_DPD_LAST_3_ACTIVE_MONTHS,
ATTEMPT_6M_CNT,MIN_MONTHS_DPD_0,CONNECT_3M_CNT,ATTEMPT_9M_CNT,CONNECT_6M_CNT,
CONTACT_CLIENT_RATE_6M,ATTEMPT_12M_CNT,CONTACT_CLIENT_RATE_3M,MONTHS_DPD_0_CUS_JOURNEY,CONNECT_9M_CNT,
CONTACT_CLIENT_RATE_9M,MIN_INCRE_LAST_MONTH,MAX_DPD_EVER,CONTACT_CLIENT_RATE,ATTEMPT_24M_CNT,CONTACT_CLIENT_RATE_24M,
PRIOR_DUE_LAST_MONTH,CONTACT_CLIENT_RATE_12M,ATTEMPT_CNT,CONNECT_12M_CNT,CONNECT_RATE_3M,CONNECT_RATE_6M,
MONTHS_DPD_10_CUS_JOURNEY,M_DEFAULT,
PRIOR_DUE_LAST_3MONTHS,CONNECT_RATE_9M,MAX_DPD_LAST_3_6_ACTIVE_MONTHS,
CONNECT_RATE,CONNECT_RATE_12M,CONTACT_3M_CNT,t4.UTILIZATION_PERCENT,CONTACT_6M_CNT,M_PRIOR_DUE_LAST_3MONTHS,
--MAX_MOB_DUE,
--MIN_MOB_DUE,
MAX_DPD_LAST_6_12_ACTIVE_MONTHS,CONTACT_CLIENT_3M_CNT,MAX_ARR_ENR_LAST_3M,MAX_ARR_LAST_3M,
CONTACT_9M_CNT,
MAX_M_DEFAULT,
BAD_CONTACT_RESPONSE_9M_PCT,BAD_ATTEMPT_RESPONSE_9M_PCT,BAD_CONNECT_RESPONSE_9M_PCT,
BAD_GOOD_RESPONSE_9M_PCT,BAD_RESPONSE_9M_CNT,BAD_ATTEMPT_RESPONSE_12M_PCT,PRIOR_DUE_PREV_MONTH,
BAD_GOOD_RESPONSE_12M_PCT,BAD_RESPONSE_12M_CNT,BAD_CONNECT_RESPONSE_12M_PCT,
BAD_CONTACT_RESPONSE_12M_PCT,PRIOR_DUE_LAST_3_6MONTHS,MIN_INCRE_PREV_MONTH,BAD_CONTACT_RESPONSE_6M_PCT,
BAD_GOOD_RESPONSE_6M_PCT,BAD_ATTEMPT_RESPONSE_6M_PCT,BAD_RESPONSE_6M_CNT,BAD_CONNECT_RESPONSE_6M_PCT,
BAD_RESPONSE_CNT,BAD_CONNECT_RESPONSE_CNT,BAD_ATTEMPT_RESPONSE_CNT,BAD_GOOD_RESPONSE_CNT,MIN_EARLY_PMT_PAY_MAX,
M_MIN_INCRE_LAST_3MONTHS,BAD_RESPONSE_3M_CNT,BAD_ATTEMPT_RESPONSE_3M_PCT,BAD_CONNECT_RESPONSE_3M_PCT,
BAD_GOOD_RESPONSE_3M_PCT,BAD_CONTACT_RESPONSE_3M_PCT,GOOD_RESPONSE_3M_CNT,GOOD_RESPONSE_6M_CNT,
M_PRIOR_DUE_LAST_3_6MONTHS,CONTACT_CLIENT_9M_CNT,CONTACT_CNT,MAX_ARR_ENR_LAST_3_6M,MAX_ARR_LAST_3_6M,GOOD_RESPONSE_9M_CNT,
MAX_CF_AMD_LAST_3_6M,MAX_DPD_LAST_12_24_ACTIVE_MONTHS,CONTACT_CLIENT_12M_CNT,CONTACT_CLIENT_CNT,
MONTH_CONTACT_CLIENT_3M,GOOD_RESPONSE_12M_CNT,AVG_ENR_RATIO_LAST_3M,MONTH_CONTACT_CLIENT_6M,MAX_ARR_LAST_6_12M,
M_MIN_INCRE_LAST_3_6MONTHS,AVG_ENR_ALL_LAST_3M,MONTH_CONTACT_CLIENT_9M,AVG_ENR_RATIO_LAST_3_6M,GOOD_RESPONSE_CNT,
MONTH_CONTACT_CLIENT_12M,MAX_CF_AMD_EVER,MAX_CF_AMD_LAST_6_12M,MAX_AMD_ALL_EVER,MAX_CF_ALL_LAST_3_6M,
MAX_EARLY_PMT_PAY_MIN,MAX_EARLY_PMT_PAY_MAX,MIN_MONTHS_JOURNEY,MAX_MONTHS_DPD_0,AVG_ENR_RATIO_LAST_6_12M,
MAX_CF_AMD_LAST_3M,MAX_CF_ALL_LAST_3M,PRIOR_STATEMENT_LAST_3MONTHS,M_PRIOR_STATEMENT_LAST_3MONTHS,PRIOR_STATEMENT_LAST_3_6MONTHS,
t4.credit_limit_amt, t4.utilization,t3.cli_amt,t3.CLI_FLAG,M_IPP_ACT_AMT_9M,S_IPP_ACT_AMT_9M,M_CASH_ACT_AMT_9M,
M_CASH_ACT_9M,S_CASH_ACT_AMT_9M,S_CASH_ACT_9M,S_CASH_ACT_AMT_6M,M_CASH_ACT_AMT_6M,S_CASH_ACT_6M,M_CASH_ACT_6M,
S_IPP_ACT_9M,M_IPP_ACT_9M,M_CASH_ACT_3M,S_CASH_ACT_3M,S_CASH_ACT_AMT_3M,M_CASH_ACT_AMT_3M,t3.CLI_AMT CLI_AMT_LIST ,t3.CLI_CNT CLI_CNT_LIST,
t3.CLI_FLAG CLI_FLAG_LIST,dcard_fg,M_IPP_AMT_SAVING9M,S_IPP_AMT_SAVING9M,S_CASH_AMT_SAVING9M,
M_CASH_AMT_SAVING9M,M_IPP_AMT_SAVING6M,S_IPP_AMT_SAVING6M,S_CASH_AMT_SAVING6M,M_CASH_AMT_SAVING6M,
CRLIM_SAVING,CRLIM_INCOME,S_CASH_AMT_SAVING3M,M_CASH_AMT_SAVING3M,S_IPP_AMT_SAVING3M,
M_IPP_AMT_SAVING3M,WITHOUT_DUE_FLAG,PRIOR_STATEMENT_LAST_MONTH,MIN_INCRE_PERCENT_3MONTHS,MIN_INCRE_PERCENT_LAST_MONTH,
FULL_DUE_INCRE_PERCENT_LAST_MONTH,PMT_WITHOUT_3MONTHS,M_PMT_WITHOUT_LAST_3MONTHS,WITHIN_DUE_FLAG,FULL_DUE_INCRE_PERCENT_LAST_3MONTHS,
PMT_WITHOUT_LAST_MONTH,PRIOR_STATEMENT_PREV_MONTH,PRIOR_CYCLE_3MONTHS,PRIOR_CYCLE_LAST_MONTH,MIN_INCRE_PERCENT_PREV_MONTH,
M_PRIOR_CYCLE_LAST_3MONTHS,PRIOR_CYCLE_PREV_MONTH,PMT_WITHOUT_PREV_MONTH,MMIN_INCRE_PERCENT_3MONTHS,FULL_DUE_INCRE_PERCENT_PREV_MONTH,
MIN_INCRE_PERCENT_3_6MONTHS,M_PMT_WITHOUT_3_6MONTHS,PMT_WITHOUT_3_6MONTHS,M_PRIOR_CYCLE_3_6MONTHS,FULL_DUE_INCRE_PERCENT_3MONTHS,
MIN_INCRE_PERCENT_CYCLE_3MONTHS,PRIOR_CYCLE_3_6MONTHS,FULL_DUE_INCRE_PERCENT_3_6MONTHS,MIN_INCRE_PERCENT_CYCLE_LAST_MONTH,
M_MIN_INCRE_PERCENT_3_6MONTHS, a.due_date
--aa.target target1,
--dpd_max, case when dpd_max > 2 then 1 else 0 end as target2
--t.DCARD_INCREASE_FLAG
--select /*+PARALLEL(+64)*/count(*)
from pol_tru_crc_main_daily_1 a
--left join pol_predue_crc_main_daily_3 a2 on a.appl_id = a2.appl_id and a.run_date = a2.run_date
--left join pol_crc_new_model_main1_newtarget_maxinmonth bb on a.appl_id = bb.appl_id and a.run_date = bb.run_date
--left join pol_tru_crc_new_model_main1_newtarget aa on a.appl_id = aa.appl_id and a.run_date = aa.run_date
--left join pol_tru_crc_pre_new_model_clivar1 t on a.appl_id = t.appl_id and a.run_date = t.run_date
left join pol_tru_crc_main_daily_raw_beh y on a.appl_id = y.appl_id and a.run_date = y.run_date
left join pol_predue_crc_main_daily_pcb b on a.appl_id = b.appl_id and a.run_date = b.run_date
left join pol_predue_crc_main_daily_col x on a.appl_id = x.appl_id and a.run_date = x.run_date
left join pol_predue_crc_main_daily_m_default c on a.id_no = c.id_no and a.run_date = c.run_date
--left join (select appl_id, dpd1 from pol_tru_crc_pre_varcheck5) e on a.appl_id = e.appl_id
left join pol_predue_crc_main_daily_maxdpd  e on a.appl_id = e.appl_id and a.run_date = e.run_date
--left join pol_tru_crc_pre_cash_retail_ipp_new_model q on a.appl_id = q.appl_id and a.run_date = q.run_date
LEFT join pol_predue_crc_main_daily_spend  a1 on a.appl_id = a1.appl_id and a.run_date = a1.run_date
LEFT join pol_predue_crc_main_daily_cli_raw_final1  t1  on a.appl_id = t1.appl_id and a.run_date = t1.run_date
LEFT join pol_predue_crc_main_daily_cash_act_amt t2 on a.appl_id = t2.appl_id and a.run_date = t2.run_date
LEFT join pol_predue_crc_main_daily_cli_list_final t3 on a.appl_id = t3.appl_id and a.run_date = t3.run_date
left join pol_predue_crc_main_daily_utilization t4 on a.appl_id = t4.appl_id and a.run_date = t4.run_date
left join pol_predue_crc_main_daily_newvar t5 on a.appl_id = t5.appl_id and a.run_date = t5.run_date);
commit;


insert into pol_tbl_predue_crc_job_control_test
(select 'pol_tru_crc_main_daily_final_var',sysdate,'Ready' from dual);
commit;

  execute immediate 'create table predue_crc_input_test'||to_char(sysdate,'ddmmyy')||' as
  select a.* from
  (select a.*,dense_rank () over(order by due_date desc) ordering from pol_tru_crc_main_daily_final_var_test a) a
  where ordering = 1';

insert into predue_crc_input_latest_update
(select trunc(sysdate) from dual);
commit;

end;
/
