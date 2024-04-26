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

gp_dict = {}
monitor_ary = []

isQdpQry = False
isAtpQry = False

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
        if self.login_status:
            print('QDP执行QryInvestorPosition查询')
            req: dict = {
                "BrokerID": self.brokerid,
                "InvestorID": self.investorid,
                "UserID": self.userid
            }
            self.reqid += 1
            self.reqQryInvestorPosition(req, self.reqid)

    ##查询持仓回调
    def onRspQryInvestorPosition(self, data: dict, error: dict, reqid: int, last: bool)->None:
        print('##QDP onRspQryInvestorPosition##')
        if error["ErrorID"]:
            isQdpQry = False
            print('QDP onRspQryInvestorPosition failed', error)
            return
        if data:
            if data['InstrumentID'] in monitor_ary:
                print('Position=',data['Position'])
                gp_dict[data['InstrumentID']] = gp_dict.get(data['InstrumentID'], {})
                gp_dict[data['InstrumentID']]['QPosition'] = data['Position']
                isQdpQry = True
        else:
            print("The data dictionary is empty.")

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
        if self.login_status:
            print('ATP执行QryInvestorPosition查询')
            req: dict = {
                "BrokerID": self.brokerid,
                "InvestorID": self.userid
            }
            self.reqid += 1
            self.reqQryInvestorPosition(req, self.reqid)


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
        if error["ErrorID"]:
            isAtpQry = False
            print('Atp onRspQryInvestorPosition failed', error)
            return
        if data:
            if data['InstrumentID'] in monitor_ary:
                print('Position=',data['Position'])
                gp_dict[data['InstrumentID']] = gp_dict.get(data['InstrumentID'], {})
                gp_dict[data['InstrumentID']]['APosition'] = data['Position']
                isAtpQry = True
        else:
            print("The data dictionary is empty.")


    def onRspQryTradingAccount(self, data: dict, error: dict, reqid: int, last: bool)->None:
        ##data对应CThostFtdcTradingAccountField结构, error对应CThostFtdcRspInfoField
        print('##onRspQryTradingAccount##')
        for i,j in data.items():
            print(i,j)
        print('######end#####')


def queryTimer(qTdApi, aTdApi):
    if qTdApi.login_status and aTdApi.login_status:
        now = datetime.now()
        time_string = now.strftime("%Y-%m-%d %H:%M:%S")
        print('当前查询时间:',time_string)
        qTdApi.qryInvestorPosition()
        aTdApi.qryInvestorPosition()

    ##时间保持一定的差值，基本能确保统一查询之后，两边都已经返回数据
    delQryCom = config.get('Time', 'delqrycom')
    comTimer = threading.Timer(delQryCom, positionCompare)
    comTimer.start()
    qryTimer = threading.Timer(2*delQryCom, queryTimer, args=(qTdApi, aTdApi))
    qryTimer.start()



def positionCompare():
    if isAtpQry and isQdpQry:
        print('开始持仓比对')
        for key in gp_dict.keys():
            if gp_dict[key].get('APosition',0) != gp_dict[key].get('QPosition',0):
                print(f'合约{key}持仓不一致,ATP持仓:{gp_dict[key].get("APosition",0)},QDP持仓:{gp_dict[key].get("QPosition",0)}')
                kill_process('zzz')
        print('持仓比对结束')


def kill_process(process_name):
    # 遍历所有进程，查找名称匹配的进程
    for proc in psutil.process_iter(['pid', 'name']):
        if process_name in proc.info['name']:
            # 使用terminate()方法终止进程
            print("kill process:",proc.info['name'])
            proc.terminate()


if __name__ == '__main__':

    config.read('config.ini')
    processName = config.get('KillProcess', 'name')
    if not processName and processName == '':
        print('please config the [KillProcess] in config.ini file')
        sys.exit(0)

    mointerStr = config.get('MoniterInstrument', 'instruments')
    if not mointerStr and mointerStr == '':
        print('please config the [MoniterInstrument] in config.ini file')
        sys.exit(0)

    monitor_ary = mointerStr.split(',')

    q_TdApi = QdpTdApi()
    q_userid = config.get('QDP', 'userid')
    q_password = config.get('QDP', 'password')
    q_brokerid = config.get('QDP', 'brokerid')
    q_investorid = config.get('QDP', 'investorid')
    q_td_address = config.get('QDP', 'tradeaddress')
    q_TdApi.connect(q_td_address, q_userid, q_password, q_brokerid, q_investorid)

    a_TdApi = AtpTdApi()
    a_investorid = config.get('ATP', 'investorid')
    a_brokerid= config.get('ATP', 'brokerid')
    a_password= config.get('ATP', 'password')
    a_td_address= config.get('ATP', 'tradeaddress')
    a_TdApi.connect(a_td_address, a_investorid, a_password, a_brokerid)

    qryTimer = threading.Timer(2, queryTimer, args=(q_TdApi, a_TdApi))
    qryTimer.start()

    input()
    sys.exit(0)