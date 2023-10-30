# *************************************************************************************************
# Scripts Name          : 
# Program Version       : V1.00
# Description           : 10号报表
# Rules of use          : 
# Author                : luxu
# Create Date           : 2023/6/5
# **************************************
import os
import pandas

from business.luxu.report_sham import sqltoDF
from common.paser.paserini import Paser
from config import ROOT_PATH
from pandasql import sqldf


class Report10():
    """

    """

    def __init__(self, ds='2023-06-02'):
        """
        ds: YY-MM-DD
        :param ds:
        """
        self.ds = ds

    @property
    @sqltoDF
    def table_trade_view_for_app(self):
        """
        :return:
        """
        return f"""SELECT '{self.ds}'                                                                AS init_date
     ,trade_date                                              AS trade_date
     , clear_date                                           AS clear_date
     ,substr(asset_account_id,2)                                                                     as fund_account
     ,account_owner_id
     , case when market='1' then 'K' when market='2' then 'P' when market='4' then 'TV' when market='8' then 'SA' else market end as exchange_type
     ,market
     , security_code AS stock_code
     ,case when money_type='1' then 'CNY' when  money_type='2' then 'HKD' when  money_type='4' then 'USD' when money_type='8' then 'SGD' when money_type='16' then 'MYR' when money_type='32' then 'SAR' else money_type end as money_type 
     , entrust_side
     , trade_platform
     , gloss_amount 
     ,if(t.entrust_side = 'B', t.ibond_interest * - 1, t.ibond_interest)      as accrued_interest                                                          
     ,CASE WHEN entrust_side IN (1,3) THEN 'B'
                     WHEN entrust_side IN (2,4) THEN 'S'  ELSE null END AS entrust_bs
     , fare0                                                                      
     , fare1                                                                       
     , fare2                                                                       
     , fare3                                                                      
     , fare4                                                                     
     , fare5                                                                      
     , fare6                                                                     
     , fare7                                                                     
     , fare8                                                                       
     , fare9                                                                     
     , farex                                                                       
     , farey                                                                      
     , settle_amount                                                               
     , 'trade_success_detail'                                                        AS source_table
from sahm_ods.ods_ksa_settle_trade_view_for_app_di t
where ds = '{self.ds}'
  and status <> -99"""

    @property
    @sqltoDF
    def table_acc_base_money_account(self):
        """

        :return:
        """
        return f"""SELECT  id
               ,money_account_category
        FROM sahm_ods.ods_ksa_borker_base_money_account_df
        WHERE ds = '{self.ds}'
        """

    @property
    @sqltoDF
    def table_pty_account_owner(self):
        """"""
        return f"""
        SELECT  id        AS account_owner_id
               ,account_owner_status -- 账户状态
        FROM sahm_ods.ods_ksa_broker_account_owner_df
        WHERE ds = '{self.ds}'"""

    def mergetable(self):
        t = self.table_trade_view_for_app
        c = self.table_acc_base_money_account
        o = self.table_pty_account_owner
        sql = f"""
        SELECT '{self.ds}'                                                                 AS init_date
     , t.trade_date                                AS trade_date
     , t.clear_date                               AS clear_date
     , t.exchange_type                                                                    AS market
     , CASE WHEN length(t.stock_code) = 21 THEN 'option' ELSE 'stock' END                 AS stock_type
     , t.money_type
     , o.account_owner_status                                                             AS account_status
     , CASE
           WHEN c.money_account_category = 1 THEN 'C'
           WHEN c.money_account_category = 2 THEN 'M'
           WHEN c.money_account_category = 3 THEN 'Comp'
           ELSE null END                                                                  AS client_type
     , t.entrust_bs
     , CASE
           WHEN t.trade_platform = 1 THEN 'HKEX'
           WHEN t.trade_platform = 2 THEN 'PHILLIP'
           WHEN t.trade_platform = 5 THEN 'IB'
           WHEN t.trade_platform = 6 THEN 'VT'
           WHEN t.trade_platform = 7 THEN 'LEK'
           WHEN t.trade_platform = 12 then 'TDWUL'
           ELSE null END                                                                  AS broker
     , coalesce(SUM(t.gloss_amount), 0)                                                   AS gross_amount
     , coalesce(SUM(t.accrued_interest), 0) AS accrued_interest
     , coalesce(SUM(t.fare0), 0)                                                          AS fare0
     , coalesce(SUM(t.fare1), 0)                                                          AS fare1
     , coalesce(SUM(t.fare2), 0)                                                          AS fare2
     , coalesce(SUM(t.fare3), 0)                                                          AS fare3
     , coalesce(SUM(t.fare4), 0)                                                          AS fare4
     , coalesce(SUM(t.fare5), 0)                                                          AS fare5
     , coalesce(SUM(t.fare6), 0)                                                          AS fare6
     , coalesce(SUM(t.fare7), 0)                                                          AS fare7
     , coalesce(SUM(t.fare8), 0)                                                          AS fare8
     , coalesce(SUM(t.fare9), 0)                                                          AS fare9
     , coalesce(SUM(t.farex), 0)                                                          AS farex
     , coalesce(SUM(t.farey), 0)                                                          AS farey
     , coalesce(SUM(t.settle_amount), 0)                                                  AS net_amount
--      , coalesce(SUM(t.gross_buy), 0)                                                      AS gross_buy
--      , coalesce(SUM(t.gross_sell), 0)                                                     AS gross_sell
--      , coalesce(SUM(if(t.entrust_bs = 'B' AND t.fare1 = 0, t.gloss_amount, 0)), 0)        AS bought
--      , coalesce(SUM(if(t.entrust_bs = 'S' AND t.fare1 = 0, t.gloss_amount, 0)), 0)        AS sold
     , t.source_table                                                                     AS source_table 
        from t  
        left join c 
        on t.fund_account = c.id
        left join  o
        on t.account_owner_id = o.account_owner_id
        WHERE ((t.exchange_type = 'K' and t.trade_platform in (1, 2)) or (t.exchange_type = 'P' and t.trade_platform in (5, 6,7)) or (t.exchange_type = 'SA' and t.trade_platform = 12))

        GROUP BY  
             t.trade_date
             ,t.clear_date
             ,t.exchange_type
             ,CASE WHEN length(t.stock_code) = 21 THEN 'option'  ELSE 'stock' END
             ,t.money_type
             ,o.account_owner_status
             ,c.money_account_category
             ,t.entrust_side
             ,t.trade_platform
             ,t.source_table; 
        
        """
        return sqldf(sql, env=locals())


if __name__ == "__main__":
    obj = Report10()
    df = obj.mergetable()
    pandas.DataFrame(df).to_csv("merge.csv",mode='w',index=False)
    df2=pandas.DataFrame(obj.table_trade_view_for_app)
    # print(sum([ float(i) for i in  df2['gloss_amount'].tolist()]))
    df2.to_csv("view_for_app.csv",mode='w',index=False)