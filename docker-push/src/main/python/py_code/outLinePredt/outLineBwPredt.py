# coding=UTF-8
'''
出口带宽预测
Created on   2018/7/20
@author:  szhong
1)读取网络设备
'''


import pandas as pd
import numpy as np
from fbprophet import Prophet

import os
import pymongo
import datetime

# 获取配置
appPath = os.path.dirname(__file__)
appConf = {}
fd = open(appPath + '/pyapp.conf', 'r')
for v in fd:
    if v.find('#') < 0:
        v = v.replace('\n', '').replace('\r', '')
        v_list = v.split('=')
        appConf[v_list[0]] = v_list[1]
fd.close()


def forecast_fit(df):
    # 拟合数据，返回模型
    df_new = pd.DataFrame()
    df_new['ds'] = df.index
    df_new['y'] = df.values

    model = Prophet(changepoint_prior_scale=0.5)
    model.fit(df_new)
    return model


def forecast_test(model, df, forecast_time, freq_sampling):
    # 给出预测时间点
    future = model.make_future_dataframe(periods=forecast_time, freq=freq_sampling)
    # 预测
    forecast = model.predict(future)

    # model.plot(forecast, xlabel='date_stamp', ylabel='flows')
    # plt.legend(['original', 'fit'])
    # model.plot_components(forecast)

    df_forecast = pd.DataFrame(forecast[['yhat', 'yhat_upper', 'yhat_lower']].values, index=forecast['ds'],
                               columns=['yhat', 'yhat_upper', 'yhat_lower'])

    # yhatdf = df_forecast[-8:][['yhat', 'yhat_upper', 'yhat_lower']]
    yhatdf = df_forecast[-forecast_time:][['yhat', 'yhat_upper', 'yhat_lower']]
    #print("yhatdf:", yhatdf)

    yhdfList = np.array(yhatdf).tolist()
    tsList = yhatdf.index.tolist()
    # yhatList = yhatdf['yhat'].values.tolist()
    # print("yhdfList:", yhdfList)
    # print("tsList:", tsList)
    # print("yhatList:", yhatList)
    return tsList, yhdfList


def main():

    try:
        # 等最新的spark作业处理完
        # time.sleep(180)
        # 1.连接数据库
        mongodbhosts = appConf['mongodbhosts']
        connection = pymongo.MongoClient('mongodb://' + mongodbhosts)
        db = connection.ion
        coll = db['outLineSumInfo']
        coll_res = db["outLineSumPredt"]

        # 2.获取各区域各服务商的出口流量,进行预测
        nowTime = datetime.datetime.utcnow()
        # tenMinAgo = nowTime - datetime.timedelta(minutes=10)
        twelveHourAgo = nowTime - datetime.timedelta(hours=3)
        # oneDayAgo = nowTime - datetime.timedelta(days=1)
        # oneWeekBefore = nowTime -datetime.timedelta(days=7)
        # print("twelveHourAgo:", twelveHourAgo)
        print("nowTime:", nowTime)

        dcList = list(coll.find({'ts': {'$gte': twelveHourAgo, '$lte': nowTime}, 'count': {"$gt": 0}}, {'ts': 1, 'items': 1}).sort("ts", 1))

        zoneIdServvMap = {}
        zoneIdServvMap["_id"] = dcList[-1]["_id"]
        zoneIdServvMap["items"] = dcList[-1]["items"]

        print("dcList[-1]ts:", dcList[-1]["ts"])
        # print("dcList[0]ts:", dcList[-0]["ts"])

        for item in dcList[-1]["items"]:
            itemKey = item["zoneIds"] + "_" + item["serviceProvider"]
            zoneIdServvMap[itemKey] = {}
            zoneIdServvMap[itemKey]["ts"] = []
            zoneIdServvMap[itemKey]["octets"] = []

        # for vkey in zoneIdServvMap:
        #     print("vkey:",vkey)
        #     for vvkey in zoneIdServvMap[vkey]:
        #         print(vvkey,zoneIdServvMap[vkey][vvkey])

        for vdc in dcList:
            # print("vdc:", vdc)
            for item in vdc["items"]:
                # print("item", item)
                itemKey = item["zoneIds"] + "_" + item["serviceProvider"]
                # print("itemKey:", itemKey)
                # if(itemKey in zoneIdServvMap.keys):（会报错，当key只有一个时）
                if(zoneIdServvMap.get(itemKey)):
                    zoneIdServvMap[itemKey]["ts"].append(vdc["ts"])
                    zoneIdServvMap[itemKey]["octets"].append(item["octets"])

        # for vkey in zoneIdServvMap:
        #     print("vkey:",vkey)
        #     for vvkey in zoneIdServvMap[vkey]:
        #         print(vvkey,zoneIdServvMap[vkey][vvkey])

        # # 实际数据小于1天则不预测
        freq_sampling = '600S'
        forecast_time = 12
        predictItems = []

        for vKey in zoneIdServvMap:
            if (vKey != "_id" and vKey != "items"):
                # print(len(zoneIdServvMap[vKey]["ts"]))
                if (len(zoneIdServvMap[vKey]["ts"]) >= 12):
                    vKeyData = zoneIdServvMap[vKey]
                    # print("vKeyData:",vKeyData)
                    # print(type(vKeyData))
                    df = pd.DataFrame(data=vKeyData)
                    df = df.drop_duplicates('ts')
                    df = df.set_index('ts')
                    df = df.sort_index()

                    df_train = df[: -forecast_time]
                    df_test = df[-forecast_time:]

                    # 拟合
                    model = forecast_fit(df_train)
                    # 测试
                    tsList, yhdfList = forecast_test(model, df, forecast_time, freq_sampling)

                    for vItem in zoneIdServvMap["items"]:
                        itemKey = vItem["zoneIds"] + "_" + vItem["serviceProvider"]
                        if itemKey == vKey:
                            vItem["yhdfList"] = yhdfList
                            break

        # 更新数据库数据
        # print(zoneIdServvMap["items"])
        # print(zoneIdServvMap["_id"])
        coll.update_one({"_id": zoneIdServvMap["_id"]}, {"$set": {"items": zoneIdServvMap["items"]}})

    finally:
        connection.close()
        endTime = datetime.datetime.utcnow()
        spendTime = endTime - nowTime
        print("spendTime:", spendTime)


if __name__ == '__main__':
    main()
