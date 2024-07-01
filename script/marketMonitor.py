import sys
import os
import time
import psutil
import threading
import configparser

sys.path.append('../lib/')

from datetime import datetime

from vnqdptd import TdApi as QTdApi
from vnatptd import TdApi as ATdApi

#qdp持仓
gp_dict = {}
#atp持仓
ap_dict = {}

isQdpQry = False
isAtpQry = False
isCancelOrdering = False
exceptionCount = 0
kill_processName = ""

config = configparser.ConfigParser()

class QdpTdApi(QTdApi):

    def __init__(self):
        """Constructor"""
        super(QdpTdApi, self).__init__()

        self.reqid = 0
        self.order_ref = 0

        self.connect_status = False
        self.login_status = False

        self.userid = ""
        self.password = ""
        self.brokerid = ""
        self.investorid = ""

        self.frontid = 0
        self.sessionid = 0

        self.order_data = []
        self.trade_data = []
        self.positions = {}
        self.sysid_orderid_map = {}

    def onFrontConnected(self):
        """
        Callback when front server is connected.
        """
        print('QDP OnFrontConnected')
        self.login()

    def onFrontDisconnected(self, reason: int):
        """
        Callback when front server is disconnected.
        """
        self.connect_status = False
        self.login_status = False
        print(f"QDP OnFrontDisconnected {reason}")

    def onRspUserLogin(self, data: dict, error: dict, reqid: int, last: bool):
        """
        Callback when user is logged in.
        """
        if not error["ErrorID"]:
            self.login_status = True
            print('QDP OnRspUserLogin success')
        else:
            print('QDP OnRspUserLogin failed', error)


    def connect(self, address: str, userid: str, password: str, brokerid: str, investorid: str):
        """
        Start connection to server.
        """
        self.userid = userid
        self.password = password
        self.brokerid = brokerid
        self.investorid = investorid

        # If not connected, then start connection first.
        if not self.connect_status:
            self.createFtdcTraderApi("")
            
            self.subscribePrivateTopic(1)
            self.subscribePublicTopic(1)

            self.registerFront(address)
            self.init()

            self.connect_status = True
        # If already connected, then login immediately.
        elif not self.login_status:
            self.login()

    def login(self):
        """
        Login onto server.
        """
        req = {
            "UserID": self.userid,
            "Password": self.password,
            "BrokerID": self.brokerid
        }
        self.reqid += 1
        self.reqUserLogin(req, self.reqid)


    ##查询持仓
    def qryInvestorPosition(self)->None:
        global isQdpQry
        isQdpQry = False
        gp_dict.clear()
        if self.login_status:
            print('QDP执行QryInvestorPosition查询')
            req: dict = {
                "BrokerID": self.brokerid,
                "InvestorID": self.investorid,
                "UserID": self.userid
            }
            self.reqid += 1
            self.reqQryInvestorPosition(req, self.reqid)

    ##查询订单
    def qryOrder(self)->None:
        if self.login_status:
            print('QDP执行QryOrder查询')
            req: dict = {
                "BrokerID": self.brokerid,
                "InvestorID": self.investorid,
                "UserID": self.userid
            }
            self.reqid += 1
            self.reqQryOrder(req, self.reqid)


    ##查询持仓回调
    def onRspQryInvestorPosition(self, data: dict, error: dict, reqid: int, last: bool)->None:
        print('##QDP onRspQryInvestorPosition##')
        global isQdpQry
        if error["ErrorID"]:
            isQdpQry = False
            print('QDP onRspQryInvestorPosition failed', error)
            return
        if data:
            print(f"InstrumentID= {data['InstrumentID']} , Direction= {data['Direction']} , Position= {data['Position']}")
            gp_dict[data['InstrumentID']] = gp_dict.get(data['InstrumentID'], {})
            gp_dict[data['InstrumentID']]['QPosition'] =  gp_dict[data['InstrumentID']].get('QPosition', 0)
            if(int(data['Direction']) == 0):
                gp_dict[data['InstrumentID']]['QPosition'] += int(data['Position'])
            else:
                gp_dict[data['InstrumentID']]['QPosition'] -= int(data['Position'])
            if last:
                isQdpQry = True
        else:
            print("The data dictionary is empty.")


    def onRspQryOrder(self, data: dict, error: dict, reqid: int, last: bool)->None:
        if error["ErrorID"]:
            print('QDP onRspQryOrder failed', error)
            return
        if data:
            if data['OrderStatus'] != '0' and data['OrderStatus'] != '5':
                print("QDP rspOrder=",data)
                qdp_req: dict = {
                    "BrokerID": self.brokerid,
                    "InvestorID": self.investorid,
                    "UserID": self.userid,
                    "ActionFlag": '0',
                    "InstrumentID":data['InstrumentID'],
                    "ExchangeID":'APEX',
                    "OrderSysID":data['OrderSysID'],
                    "UserOrderLocalID":data['UserOrderLocalID']
                }
                print('QDP reqOrderAction 撤单, ', qdp_req)
                self.reqid += 1
                self.reqOrderAction(qdp_req, self.reqid) 


    def close(self):
        """
        Close the connection.
        """
        if self.connect_status:
            self.exit()



class AtpTdApi(ATdApi):
    def __init__(self)->None:
        super().__init__()

        self.reqid: int = 0
        self.order_ref: int = 0

        self.connect_status: bool = False
        self.login_status: bool = False
        self.contract_inited: bool = False

        self.userid: str = ""
        self.password: str = ""
        self.brokerid: str = ""


    def connect(self,address:str,userid:str,password:str,brokerid:str)->None:
        self.userid = userid
        self.password = password
        self.brokerid = brokerid
        if not self.connect_status:
            path = os.path.dirname(os.path.abspath(__file__))
            self.createFtdcTraderApi((str(path) + "\\Td").encode("GBK"))
            self.subscribePublicTopic(0)
            self.subscribePrivateTopic(0)
            self.registerFront(address)
            self.init()
            self.connect_status = True
    
    
    def login(self)->None:
        if not self.login_status:
            atp_req: dict = {
                "UserID": self.userid,
                "Password": self.password,
                "BrokerID": self.brokerid,
            }
            self.reqid += 1
            self.reqUserLogin(atp_req, self.reqid)


    def close(self)->None:
        if self.connect_status:
            self.exit()
        
    ##查询持仓
    def qryInvestorPosition(self)->None:
        global isAtpQry
        isAtpQry = False
        ap_dict.clear()
        if self.login_status:
            print('ATP执行QryInvestorPosition查询')
            req: dict = {
                "BrokerID": self.brokerid,
                "InvestorID": self.userid
            }
            self.reqid += 1
            self.reqQryInvestorPosition(req, self.reqid)

    ##查询订单
    def qryOrder(self)->None:
        if self.login_status:
            print('ATP执行QryOrder查询')
            req: dict = {
                "BrokerID": self.brokerid,
                "InvestorID": self.userid
            }
            self.reqid += 1
            self.reqQryOrder(req, self.reqid)


    def qryTradingAccount(self)->None:
        if self.login_status:
            print('ATP执行qryTradingAccount')
            req: dict = {
                "BrokerID": self.brokerid,
                "InvestorID": self.userid
            }
            self.reqid += 1
            self.reqQryTradingAccount(req, self.reqid)



    def onFrontConnected(self)->None:
        print('ATP OnFrontConnected')
        self.login()
    
    
    def onFrontDisconnected(self,reason:int)->None:
        self.connect_status = False
        self.login_status = False
        print(f"ATP OnFrontDisconnected {reason}")


    def onRspUserLogin(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        ##data对应CThostFtdcRspUserLoginField, error对应CThostFtdcRspInfoField
        if not error["ErrorID"]:
            print('ATP OnRspUserLogin success')
            self.frontid = data["FrontID"]
            self.sessionid = data["SessionID"]
            self.login_status = True
            return
            print('ATP开始合约信息查询')
            while True:
                self.reqid += 1
                n: int = self.reqQryInstrument({}, self.reqid)
                if not n:
                    break
                else:
                    time.sleep(1)
        else:
            self.login_status = False
            print('ATP OnRspUserLogin failed', error['ErrorID'],error['ErrorMsg'])
    

    def onRspQryInstrument(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        ##data对应CThostFtdcInstrumentField, error对应CThostFtdcRspInfoField
        print(data['ProductClass'],data['InstrumentID'],data['ProductID'],reqid,last)
        if last:
            self.contract_inited = True
            print('合约信息查询完毕')

    ##查询持仓回调
    def onRspQryInvestorPosition(self, data: dict, error: dict, reqid: int, last: bool)->None:
        ##data对应CThostFtdcInvestorPositionField结构, error对应CThostFtdcRspInfoField
        print('##Atp onRspQryInvestorPosition##')
        global isAtpQry
        if error["ErrorID"]:
            isAtpQry = False
            print('Atp onRspQryInvestorPosition failed', error)
            return
        if data:
            print(f"InstrumentID= {data['InstrumentID']} , Direction= {data['PosiDirection']} , Position= {data['Position']}")
            if data['InstrumentID'].startswith('GC'):
                ap_dict[data['InstrumentID']] = ap_dict.get(data['InstrumentID'], {})
                ap_dict[data['InstrumentID']]['APosition'] = ap_dict[data['InstrumentID']].get('APosition', 0)
                ap_dict[data['InstrumentID']]['APosition'] += int(data['Position'])
            if last:
                isAtpQry = True
        else:
            print("The data dictionary is empty.")


    def onRspQryOrder(self, data: dict, error: dict, reqid: int, last: bool)->None:
        if error["ErrorID"]:
            print('Atp onRspQryOrder failed', error)
            return
        if data:
            if data['OrderStatus'] != '0' and data['OrderStatus'] != '5':
                print("Atp rspOrder=",data)
                atp_req: dict = {
                    "BrokerID": self.brokerid,
                    "InvestorID": self.userid,
                    "UserID": self.userid,
                    "ActionFlag": "0",
                    "ExchangeID": data['ExchangeID'],
                    "OrderSysID":data['OrderSysID'],
                    "FrontID": self.frontid,
                    "SessionID": self.sessionid
                }
                print('Atp reqOrderAction 撤单, ', atp_req)
                self.reqid += 1
                self.reqOrderAction(atp_req, self.reqid) 


    def onRspQryTradingAccount(self, data: dict, error: dict, reqid: int, last: bool)->None:
        ##data对应CThostFtdcTradingAccountField结构, error对应CThostFtdcRspInfoField
        print('##onRspQryTradingAccount##')
        for i,j in data.items():
            print(i,j)
        print('######end#####')


def queryTimer(qTdApi, aTdApi):
    global isCancelOrdering
    if qTdApi.login_status and aTdApi.login_status:
        if isCancelOrdering:
            time.sleep(60)
            isCancelOrdering = False
        now = datetime.now()
        time_string = now.strftime("%Y-%m-%d %H:%M:%S")
        print('##############################################################')
        print('当前查询时间:',time_string)
        qTdApi.qryInvestorPosition()
        aTdApi.qryInvestorPosition()

        ##时间保持一定的差值(ms),确保统一查询之后，两边都已经返回数据
        deltime = int(config.get('Time', 'deltime'))
        comTimer = threading.Timer(deltime, positionCompare,args=(qTdApi, aTdApi))
        comTimer.start()
        qryTimer = threading.Timer(2*deltime, queryTimer, args=(qTdApi, aTdApi))
        qryTimer.start()


def positionCompare(qTdApi, aTdApi):
    global isQdpQry
    global isAtpQry
    global isCancelOrdering
    global exceptionCount
    if isAtpQry and isQdpQry:
        print('开始持仓比对')
        if 'AUP1' not in gp_dict:
                gp_dict['AUP1'] = {}
                gp_dict['AUP1']['QPosition'] = 0
        if 'AUP10' not in gp_dict:
                gp_dict['AUP10'] = {}
                gp_dict['AUP10']['QPosition'] = 0
        if 'AUP100' not in gp_dict:
            gp_dict['AUP100'] = {}
            gp_dict['AUP100']['QPosition'] = 0
        Qpositions = gp_dict['AUP1']['QPosition'] + gp_dict['AUP10']['QPosition']*10 + gp_dict['AUP100']['QPosition']*100
        Apositions = 0
        for key in ap_dict.keys():
            Apositions += ap_dict[key].get("APosition",0)
        Apositions = Apositions*100

        print(f"QDP总持仓(盎司)= {Qpositions} , ATP总持仓(盎司)= {Apositions}")
        if abs(Qpositions + Apositions) > 100:
            print('持仓比对异常')
            exceptionCount +=1
            if exceptionCount >= 3:
                print('连续异常次数>=3次 , 执行杀死进程并撤单操作')
                isCancelOrdering = True
                kill_process(kill_processName)
                print('杀死进程:',kill_processName)
                time.sleep(1)
                print('开始查询订单并撤单')
                cancelAllOrders(qTdApi, aTdApi)
                exceptionCount = 0
            return
        else:
            exceptionCount = 0
        print('持仓比对结束')


def cancelAllOrders(qTdApi, aTdApi):
    qTdApi.qryOrder()
    aTdApi.qryOrder()


def kill_process(process_name):
    # 遍历所有进程，查找名称匹配的进程
    for proc in psutil.process_iter(['pid', 'name']):
        if process_name in proc.info['name']:
            # 使用terminate()方法终止进程
            print("kill process:",proc.info['name'])
            proc.terminate()


if __name__ == '__main__':

    print('start marketMonitor')
    config.read('config.ini')
    kill_processName = config.get('KillProcess', 'name')
    if not kill_processName and kill_processName == '':
        print('please config the [KillProcess] in config.ini file')
        sys.exit(0)

    tagQDP = 'QDP'
    q_TdApi = QdpTdApi()
    q_userid = config.get(tagQDP, 'userid')
    q_password = config.get(tagQDP, 'password')
    q_brokerid = config.get(tagQDP, 'brokerid')
    q_investorid = config.get(tagQDP, 'investorid')
    q_td_address = config.get(tagQDP, 'tradeaddress')
    q_TdApi.connect(q_td_address, q_userid, q_password, q_brokerid, q_investorid)

    tagATP = 'ATP'
    a_TdApi = AtpTdApi()
    a_investorid = config.get(tagATP, 'investorid')
    a_brokerid= config.get(tagATP, 'brokerid')
    a_password= config.get(tagATP, 'password')
    a_td_address= config.get(tagATP, 'tradeaddress')
    a_TdApi.connect(a_td_address, a_investorid, a_password, a_brokerid)

    while True:
        if q_TdApi.login_status and a_TdApi.login_status:
            ##此处接口要预热10s
            qryTimer = threading.Timer(10, queryTimer, args=(q_TdApi, a_TdApi))
            qryTimer.start()
            break

    while True:
        time.sleep(5)
