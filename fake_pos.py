import sys
import abc
import csv
import random
import string
import bson
import datetime

from airflow.operators.python_operator import PythonOperator
import pprint
import copy
from multiprocessing import Process, Value, Array
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
dag = DAG(
    'fake_pos',
    default_args=default_args,
    description='insert into mongodb orders test',
    schedule_interval=datetime.timedelta(days=8),
)




def calc_dt_delta(dt1, dt2):
    return datetime.datetime.combine(dt1, datetime.time.min) - datetime.datetime.combine(dt2, datetime.time.min)


class RandomPos:
    """
    A random pos, emit random pay order!
    :parameter name, name of pos
    """

    def __init__(self, seed_orders):
        """
        :param seed: order data of one day
        """
        self.seed_orders = seed_orders

    def get_orders(self, dt, num):
        '''
        :param day: time of day
        :param num: order num to generate
        :return: order list
        '''
        log_timestamp('seeding')
        orders = random.choices(self.seed_orders, k=num)
        log_timestamp('seeding end')

        new_orders = []
        for oo in orders:
            o = copy.deepcopy(oo)
            dt_delta = calc_dt_delta(o['createTime'].date(), dt)
            # ISO8583
            # ISO8583.acctNum
            # ISO8583.acctNumT
            # ISO8583.acqInsCode
            # ISO8583.authCode
            # ISO8583.forwardingIIN
            # ISO8583.issrCountry
            # ISO8583.mcc
            # ISO8583.merCode
            # ISO8583.merName
            # ISO8583.merchantCity
            # ISO8583.retrievalRefNum
            # ISO8583.termCode
            # ISO8583.traceNum
            o.pop('_id')
            # o['_id'] = bson.ObjectId()
            # acqEncCertID
            # actualChanTransAmt
            # actualChanTransCurrency
            # adjustFlag
            # agentCode
            # agentName
            # attach
            # billingAmount
            # billingCurrency
            # billingExchangeRate
            # bizTuition
            # bizTuition.billType
            # bizTuition.bizType
            # bizTuition.studentID
            # bizTuition.university
            # busicd
            # category
            # chanAPIVer
            # chanCode
            # chanCostTime
            # chanDiscount
            # chanMerId
            # chanMerShopId
            if 'chanOrderNum' in o:
                o['chanOrderNum'] = ''.join(random.choices('0123456789', k=28))
            # chanRefundAmt
            # chanRespCode
            # chanRespMsg
            # chanRspTime
            # chanSignCertID
            # chanTransAmt
            # chanTransCurrency
            if 'chanTransKey' in o:
                o['chanTransKey'] = 'Q' + ''.join(random.choices('0123456789', k=28))
            # chanVoucherNo
            # channelId
            # channelMcc
            # clearFlag
            # clientVer
            # consumerAccount
            # consumerId
            # costAmount
            o['createTime'] -= dt_delta
            # currency
            # customizedProcFlag
            # customizedRevFlag
            # discountAmt
            # discountDetails
            # discountDetails
            # .0
            # discountDetails
            # .0.discountAmt
            # discountDetails
            # .0.discountNote
            # endToEndId
            # entity
            # errorDetail
            # exchangeRate
            # fee
            # frontUrl
            # fxValue
            # goodsInfo
            # gwid
            # insCode
            # insName
            # intMerCode
            # intStoreCode
            # isRiskSent
            # isSupportPreAuth
            # lastEnquiryTime
            # localCostTime
            # lockFlag
            # managerName
            # markup
            # markupAmt
            # merDiscount
            # merName
            # merNum
            # merTxnTime
            # merchantPan
            o['msgId'] = ''.join(random.choices('0123456789abcdef', k=32))
            # netFee
            # netProductAmt
            # nickName
            # notifyUrl
            o['orderNum'] = ''.join(random.choices('0123456789abcdef', k=32))
            # origBusicd
            if 'origChanOrderNum' in o:
                o['origChanOrderNum'] = ''.join(random.choices('0123456789', k=28))
            if 'origOrderNum' in o:
                o['origOrderNum'] = ''.join(random.choices('0123456789abcdef', k=32))
            # payStatus
            if 'payTime' in o:
                o['payTime'] -= dt_delta
            # paymentBrand
            # prePayId
            # primaryChanMerId
            # procFlag
            if 'qrCode' in o:
                o['qrCode'] = ''.join(random.choices('0123456789abcdef', k=32))
            # rateDate
            # rateValue
            # realPaymentBrand
            if 'recLastModifyTime' in o:
                o['recLastModifyTime'] -= dt_delta
            # refundAmt
            # refundStatus
            # reqChanTime
            # respCode
            # revFlag
            # salesTax
            # scanPayAPIMode
            # scanPayAPIVer
            # settCurr
            # settCurrAmt
            # settRole
            # settlementConvRate
            # settlementDate
            if 'sortTime' in o:
                o['sortTime'] -= dt_delta
            # stampDuty
            # stampDutyAmt
            # storeMcc
            # storeName
            # storeNum
            # subConsumerId
            # subMerId
            # subPaymentBrand
            # subTrans
            # subTrans
            # .0
            # subTrans
            # .0.subChanOrderNum
            # subTrans
            # .0.subPayTime
            # subTrans
            # .0.subTransAmt
            # subtotalAmt
            if 'sysOrderNum' in o:
                o['sysOrderNum'] = ''.join(random.choices('0123456789abcdef', k=32))
            # termNum
            # terminalid
            # tokenNo
            # tradeFrom
            # transAmountCur
            # transAmt
            # transScene
            # transStatus
            # transType
            # transitOperatorID
            # transport
            # transport.basicAmt
            # transport.codeType
            # transport.eventType
            # transport.identityCode
            # transport.identityCodeCreateTime
            # transport.lineName
            # transport.lineNo
            # transport.orderDetail
            # transport.orderDetail
            # .0
            # transport.orderDetail
            # .0.content
            # transport.orderDetail
            # .0.lang
            # transport.orderTitle
            # transport.orderTitle
            # .0
            # transport.orderTitle
            # .0.content
            # transport.orderTitle
            # .0.lang
            # transport.stationName
            # transport.stationName
            # .0
            # transport.stationName
            # .0.content
            # transport.stationName
            # .0.lang
            # transport.stationNo
            # transport.tapNo
            # transport.tapTime
            # transport.trafficSource
            # transport.transitWallet
            # transport.tripOrderNum
            # transport.userClient
            # transport.userId
            if 'updateTime' in o:
                o['updateTime'] -= dt_delta

            new_orders.append(o)

        return new_orders

def log_timestamp(s):
    print(datetime.datetime.now().time(), s)

def task_func():
#if __name__ == '__main__':
    print(sys.version)
    print(sys.path)
    from pymongo import MongoClient

    mc = MongoClient('mongodb://mongodb-mongodb-replicaset/admin?readPreference=primary')

    seeds = [o for o in mc['trans']['double11'].find()]
    pos = RandomPos(seeds)

    now = datetime.datetime.now()
    dt = datetime.datetime.now()
    while (now - dt).days < 2:
        print(dt)
        log_timestamp('get orders')
        orders = pos.get_orders(dt.date(), 1*1000)

        log_timestamp('insert orders')
        mc['trans']['trans_1m_180day'].insert_many(orders)

        log_timestamp('insert completed')
        dt -= datetime.timedelta(1)

run_this = PythonOperator(
    task_id="insert_into_mongodb",
    provide_context=False,
    python_callable=task_func,
    dag=dag
)