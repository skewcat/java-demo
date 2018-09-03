# coding=UTF-8
'''
Created on 2018,1,24

@author: szhong
'''
from pyspark import SparkConf, SparkContext
import pymongo
import datetime

from torch.autograd import Variable

# model.py
import torch
import torch.nn as nn
from torch.nn.parameter import Parameter

# prepare.py
from scipy.signal import medfilt

# dataset.py
import os
import pandas as pd
import numpy as np
import time

# 1.获取配置
appPath = os.path.dirname(__file__)
appConf = {}
fd = open(appPath + '/pyapp.conf', 'r')
for v in fd:
    if v.find('#') < 0:
        v = v.replace('\n', '').replace('\r', '')
        v_list = v.split('=')
        appConf[v_list[0]] = v_list[1]
fd.close()

mongodbhosts = appConf['mongodbhosts']


# ----------------------------------model.py-----------------------------------------------#
STEADY_MODEL = 0
WAVE_MODEL = 1


class LSTMNet(nn.Module):
    """
    lstm模型
    """
    def __init__(self, input_size, hidden_size, output_size, num_layers, future_len):
        super(LSTMNet, self).__init__()

        self.lstm = nn.LSTM(input_size, hidden_size, num_layers)
        self.fc = nn.Linear(hidden_size, output_size)

        # lstm的初始状态也进行学习
        self.h0 = Parameter(torch.randn(num_layers, 1, hidden_size), requires_grad=True)
        self.c0 = Parameter(torch.randn(num_layers, 1, hidden_size), requires_grad=True)

        self.hidden_size = hidden_size
        self.output_size = output_size
        self.num_layers = num_layers
        # 要连续预测多少步
        self.future_len = future_len

    def forward(self, x, future_time):
        """

        :param x:  (seq_len, batch, input_size)
        :param future_time: (future_len-1, batch, 24) 未来要预测的时间长度
        :return: y (future_len, batch, output_size)
        """
        batch_size = x.size(1)

        hidden = (self.h0.expand(self.num_layers, batch_size, self.hidden_size).contiguous(),
                  self.c0.expand(self.num_layers, batch_size, self.hidden_size).contiguous())
        output, hidden = self.lstm(x, hidden)

        ys = []
        output = self.fc(output[-1, :, :].view(-1, self.hidden_size)).view(1, -1, self.output_size)
        ys.append(output)

        y = output
        for i in range(self.future_len - 1):
            now_time = future_time[i:i + 1, :, :]
            x = torch.cat([y, now_time], dim=2)

            y, hidden = self.lstm(x, hidden)
            y = self.fc(y.view(-1, self.hidden_size)).view(1, -1,
                                                           self.output_size)  # (seq_len x batch_size x output_size)
            ys.append(y)

        ys = torch.cat(ys)
        return ys


class SteadyModel:
    """稳态模型（数据基本保持不变）"""
    def __call__(self, x, pred_cnt):
        """
        :param x:  (seq_len, input_size) numpy array
        :return:
        """
        temp_mean = np.mean(x[:, 0], axis=0)
        return np.ones(pred_cnt) * temp_mean


class ClassifyModel:
    """
    分类数据模型。目前已知的有周期性的波形和平稳不变的模型
    """
    def __call__(self, x):
        """
        :param x: list or 1d numpy array
        :return:
        """
        std = np.std(x)

        # 平稳模式中温度的变化保持在一两度以内。
        if std <= 1:
            return STEADY_MODEL
        else:
            return WAVE_MODEL


# ----------------------------------dataset.py-----------------------------------------------#
import re

from torch.utils.data import Dataset

# 重采样后的数据间隔
sample_freq_s = int(appConf['sample_freq_s'])  # 单位秒
# 为了分类不同的模式，需要如下这么长的历史数据
classify_mode_look_back_hours = 24
classify_mode_look_back_sample = int(classify_mode_look_back_hours * 3600 / sample_freq_s)


class TempDataset(Dataset):
    """温度的数据集, 目前使用的相关的数据有cpu，内存"""

    def __init__(self, input_seq_len, output_seq_len, root=None, start_pert=0, end_pert=1, stride=None, transform=None,
                 classify=ClassifyModel(), exclude_steady=True, pickle=None, dataframe=None):
        """
        :param input_seq_len:    时序预测中输入数据的序列要有多长
        :param output_seq_len:   时序预测中输出数据的序列要有多长。大于1时表示连续预测
        :param root:             数据的根目录
        :param start_pert:       和end_pert一起决定从总数据集总划分多少出来。用于划分训练数据和测试数据
        :param end_pert:
        :param stride:           以多长的间隔滑动采样窗口，窗口越小，数据越多，但冗余性越大。默认是input_seq_len
        :param transform:        对数据进行转换，比如归一化。
        :param classify:         对原始数据进行模式分类。
        :param exclude_steady:   是否排除掉平稳模式的温度数据。在训练和校验的时候，需要排除掉，测试的时候，为了统一评估不同模式的数据，
                                 需要将exclude_steady设置为False
        :param pickle:           指明数据文件. 在测试时为了连续预测，需要保证数据的连续性，一个独立的pickle可以保证时间是连续的
        :param dataframe:        数据文件对应的dataframe。dataframe和pickle只能由一个为True
        """

        self._input_seq_len = input_seq_len
        self._output_seq_len = output_seq_len
        self._stride = stride
        self._start_pert = start_pert
        self._end_pert = end_pert
        self._exclude_steady = exclude_steady
        self.data = []
        self.classifier = classify

        if dataframe is not None:
            self.append_data(dataframe, self.data)
        elif pickle is not None:
            if not os.path.exists(os.path.join(root, pickle)):
                raise ValueError("pickle file not exist")

            df = pd.read_pickle(os.path.join(root, pickle))
            self.append_data(df, self.data)
        else:
            for f in os.listdir(root):
                if os.path.isdir(os.path.join(root, f)):
                    subdir = os.path.join(root, f)
                    for sf in os.listdir(subdir):
                        if re.match(r"data \d+.pkl", sf):
                            df = pd.read_pickle(os.path.join(subdir, sf))
                            self.append_data(df, self.data)
                        else:
                            continue
                elif re.match(r"data \d+.pkl", f):
                    # 测试的时候是每个ip单独测试，因此根目录就是ip目录
                    df = pd.read_pickle(os.path.join(root, f))
                    self.append_data(df, self.data)

        self.transform = transform

    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx):
        sample = self.data[idx]

        if self.transform:
            sample = self.transform(sample)

        return sample

    def append_data(self, df, data_list):
        # 由于存在数据缺失，数据的开头和结束可能是nan，中间部分由于经过插值，不会是nan。
        # 因此经过dropna后，所有的数据时间上仍然是连续的
        df = df.dropna()

        # 记录小时数
        hour = np.array(df.index.hour)

        data = np.array(df)

        # 将小时换成one-hot表示的24小时
        hours = np.zeros([len(data), 24])
        hours[range(hours.shape[0]), hour] = 1

        # 二维数据。列是[temperature, 24 hours]
        data = np.concatenate([data, hours], axis=1)

        start_idx = int(len(data) * self._start_pert)
        end_idx = int(len(data) * self._end_pert)
        data = data[start_idx:end_idx]

        if self._stride is None:
            self._stride = self._input_seq_len

        # 如果输入序列的长度小于区分不同模式所需要的长度，那么长度按
        if self._input_seq_len < classify_mode_look_back_sample:
            backward_len = classify_mode_look_back_sample
        else:
            backward_len = self._input_seq_len

        # 对数据进行分割
        split = int((len(data) - self._output_seq_len - backward_len) // self._stride + 1)
        if split <= 0:
            # 数据量过少
            return

        for i in range(split):
            split_in_data = data[self._stride * i:self._stride * i + backward_len]
            split_out_data = data[
                             self._stride * i + backward_len:self._stride * i + backward_len + self._output_seq_len]

            if self._exclude_steady:
                if (self.classifier(split_in_data[:, 0]) == STEADY_MODEL):
                    continue
                else:
                    split_in_data = split_in_data[-self._input_seq_len:]

                    split_in_data = split_in_data.astype("float32")
                    split_out_data = split_out_data.astype("float32")

                    data_list.append((split_in_data, split_out_data))
            else:
                dmodel = self.classifier(split_in_data[:, 0])

                split_in_data = split_in_data[-self._input_seq_len:]

                split_in_data = split_in_data.astype("float32")
                split_out_data = split_out_data.astype("float32")

                data_list.append((split_in_data, split_out_data, dmodel))


# ----------------------------------prepare.py-----------------------------------------------#
def prepare(date_list, temp_list, sample_freq_s=sample_freq_s, future_period=24 * 3600):
    """
    将原始的记录数据文件转换为后续预测需要的数据格式
    :param file_name:       数据文件
    :param sample_freq_s:   冲裁样的时间间隔。原始数据由于采样时间可能不规整，需要重采样为规整的数据
    """
    date_temp = {}
    date_temp['real_date'] = date_list
    date_temp['temp'] = temp_list

    df = pd.DataFrame(data=date_temp)
    # df['real_date'] = pd.to_datetime(df['real_date'], format="%Y-%m-%d_%H:%M:%S")
    df = df.drop_duplicates('real_date')
    df = df.set_index('real_date')

    # 标准化时间
    start_timestamp = df.index[0].value / 1e9
    end_timestamp = df.index[-1].value / 1e9

    clip_start_timestamp = ((start_timestamp - 1) // sample_freq_s) * sample_freq_s + sample_freq_s
    clip_end_timestamp = (end_timestamp // sample_freq_s) * sample_freq_s

    if clip_end_timestamp <= clip_start_timestamp:
        return None

    start_datetime = pd.to_datetime(clip_start_timestamp, unit='s')
    end_datetime = pd.to_datetime(clip_end_timestamp, unit='s')

    new_index = pd.date_range(start=start_datetime, end=end_datetime, freq=str(sample_freq_s) + 'S')
    try:
        df = df.reindex(new_index.union(df.index)).interpolate(method='time').reindex(new_index)
    except Exception as e:
        print(str(e))
        print("preprocess failed")
        return None

    # 中值滤波去除噪声

    try:
        temp_list = np.array(df['temp'])
        df['temp'] = medfilt(temp_list, 5)
    except:
        # 可能由于数据太少，无法进行中值滤波。忽略这样少的数据即可
        return None

    future_index = pd.date_range(start=end_datetime, freq=str(sample_freq_s) + 'S',
                                 periods=int(future_period / sample_freq_s) + 1, closed='right')
    future_hour = future_index.hour
    one_hot_hour = np.zeros((len(future_hour), 24))
    one_hot_hour[range(len(future_hour)), future_hour] = 1

    return df, one_hot_hour

class Normalizer:
    def __init__(self, in_mean, in_std, if_num_range, out_mean=None, out_std=None):
        self.in_mean = in_mean
        self.in_std = in_std
        self.out_mean = out_mean
        self.out_std = out_std
        self.if_num_range = if_num_range

    def __call__(self, sample):
        # 根据dataset是否接受平稳模式(exclude_steady)，返回的数据有两种格式

        if len(sample) == 1:
            input = sample
            input = np.copy(input)

            input[:, :self.if_num_range] = (input[:, :self.if_num_range] - self.in_mean) / self.in_std

            return input.astype("float32")

        if len(sample) == 2:
            input, output = sample
            input = np.copy(input)
            output = np.copy(output)

            input[:, :self.if_num_range] = (input[:, :self.if_num_range] - self.in_mean) / self.in_std

            if self.out_mean is not None and self.out_std is not None:
                output[:, :self.if_num_range] = (output[:, :self.if_num_range] - self.out_mean) / self.out_std

            return input.astype("float32"), output.astype("float32")
        elif len(sample) == 3:
            input, output, dmodel = sample

            input = np.copy(input)
            output = np.copy(output)

            input[:, :self.if_num_range] = (input[:, :self.if_num_range] - self.in_mean) / self.in_std

            if self.out_mean is not None and self.out_std is not None:
                output[:, :self.if_num_range] = (output[:, :self.if_num_range] - self.out_mean) / self.out_std

            return input.astype("float32"), output.astype("float32"), dmodel
        else:
            raise ValueError("sample length error {}".format(len(sample)))

    def reverse(self, x, type):
        """
        反归一化
        :param x: (batch, feature_size) numpy array。 feature_size必须和mean和std的长度相符
        :param type: 'in' or 'out'
        :param col: 第几列
        :return:  反归一化后的值
        """
        if type == 'out':
            mean = self.out_mean
            std = self.out_std
        elif type == 'in':
            mean = self.in_mean
            std = self.in_std
        else:
            raise ValueError("type should be 'in' or 'out'")

        return (x * std) + mean


def RMSE(y1, y2):
    """
    计算两个矩阵的rmse
    :param y1:  [N, F]的numpy 数组。 N为batch个数，F为特征数
    :param y2:  [N, F]的numpy 数组。 N为batch个数，F为特征数
    :return:    [F] 每个特征的rmse
    """

    return np.sqrt(np.mean(np.square(y1 - y2), axis=0))


def preprocess1(x):
    s1_list = list(x[1])
    date_list = []
    temp_list = []

    for j in range(len(s1_list)):
        date_list.append(s1_list[j]['ts'])
        temp_list.append(s1_list[j]['temp'])
    # print(x[0], date_list, temp_list)
    return (x[0], date_list, temp_list)


def tempPredt(partition):
    connection2 = pymongo.MongoClient('mongodb://' + mongodbhosts)
    db2 = connection2['ion']
    coll2 = db2['devTempPredict']

    # --------------------------------------------------------
    #                           加载模型
    # --------------------------------------------------------
    model = torch.load(appPath + '/lstm.model')
    # model = torch.load('lstm.model')
    # 输入序列的时间长度
    input_seq_len = model['input_seq_len']
    # 输出序列的时间长度，默认为1，即模型里只预测下一个时刻的输出
    output_seq_len = model['output_seq_len']
    input_size = model['input_size']
    hidden_size = model['hidden_size']
    output_size = model['output_size']
    normalize = model['normalize']
    num_layers = model['num_layers']

    net = LSTMNet(input_size, hidden_size, output_size, num_layers, output_seq_len)
    net.load_state_dict(model['state_dict'])
    net.eval()

    smodel = SteadyModel()

    normalizer = Normalizer(normalize['in_mean'], normalize['in_std'], 1, normalize['out_mean'], normalize['out_std'])

    for x in partition:
        # print(x[0])
        future_period = 2 * 3600
        date_list = list(x[1])
        temp_list = list(x[2])
        # print("did,len(date_list),len(temp_list):",x[0],len(date_list),len(temp_list))
        # print("date_list:",date_list)
        # print("temp_list",temp_list)
        # future_period不能直接传参!!!
        df, future_time = prepare(date_list, temp_list, future_period=future_period)
        dataset = TempDataset(input_seq_len, 0, dataframe=df, stride=1, transform=normalizer, exclude_steady=False)

        # 预测一天的数据， 由于预处理时每个间隔的数据是10分钟，因此需要144组数据
        eval_cnt = int(future_period / sample_freq_s)

        for i_f in range(len(flag_list)):
            if flag_list[i_f] == 1:
                # ==1，说明需要预测
                # 这边返回none会导致程序挂掉，需要异常处理
                #print('i_f+1:', i_f+1)
                try:
                    inp, _, data_model = dataset[-(i_f+1)]
                except:
                    continue

                # 这边笔误，SteadyModel改为STEADY_MODEL
                if data_model == STEADY_MODEL:
                    # print("Steady mode")
                    y = smodel(inp, eval_cnt)
                    y = normalizer.reverse(y, 'out')
                else:
                    # print("Wave mode")
                    y = continuous_predict(net, normalizer, inp, future_time)

                # print("ts:",date_list[-1])
                # print("did:",x[0])
                # print("y:",len(y),y)

                dc = {}
                # dc['ts'] = datetime.datetime.strptime(date_list[-1], "%Y-%m-%d_%H:%M:%S")
                dc['ts'] = date_list[-(i_f+1)]
                dc['did'] = x[0]
                dc['tempsPd'] = []
                for v in y:
                    dc['tempsPd'].append(int(round(v)))

                try:
                    coll2.insert_one(dc)
                except:
                    continue
    connection2.close()


def continuous_predict(net, normalizer, inputs, future_time):
    eval_iter_cnt = int(np.ceil(len(future_time) / net.future_len))

    input_seq_len = inputs.shape[0]
    output_seq_len = net.future_len

    ys = []
    inputs = np.expand_dims(inputs, 1)
    future_time = np.expand_dims(future_time, 1)
    inputs = Variable(torch.Tensor(inputs), volatile=True)
    for i in range(eval_iter_cnt):
        f_time = future_time[i * net.future_len:(i + 1) * net.future_len]
        f_time = Variable(torch.Tensor(f_time), volatile=True)
        y = net(inputs, f_time)

        # 更新输入
        if output_seq_len < input_seq_len:
            inputs[:-output_seq_len, 0, :] = inputs[output_seq_len:, 0, :]
            inputs[-output_seq_len:, 0, 0] = y[:, 0, 0]
            inputs[-output_seq_len:, 0, 1:] = f_time[:, 0, :]
        elif output_seq_len == input_seq_len:
            inputs[:, 0, 0] = y[:, 0, 0]
            inputs[:, 0, 1:] = f_time[:, 0, :]
        else:
            inputs[:, 0, 0] = y[-input_seq_len:, 0, 0]
            inputs[:, 0, 1:] = f_time[-input_seq_len:, 0, :]

        y = y.data.numpy()[:, 0, 0]
        y = normalizer.reverse(y, 'out')
        ys.append(y)

    return np.concatenate(ys, axis=0)


def main():

    # --------------------------------------------------------
    #                         设定前后时间（24h）
    # --------------------------------------------------------
    time.sleep(60)
    nowTime = datetime.datetime.utcnow()
    t10MinBefore = nowTime - datetime.timedelta(minutes=10)
    t26HourBefore = nowTime - datetime.timedelta(hours=26)

    # --------------------------------------------------------
    #                         连接数据库
    # --------------------------------------------------------
    connection = pymongo.MongoClient('mongodb://' + mongodbhosts)
    db = connection.ion
    coll = db['devBasicInfo']
    coll2 = db['devTempPredict']
    # 查看10min内生成的实际数据的各时间点
    # 也有可能没有新数据!!!
    ts_list = coll.distinct('ts', {'temp': {'$gt': 0}, 'ts': {'$gt': t10MinBefore, '$lte': nowTime}})
    print(ts_list)
    global flag_list

    flag_list = [1] * len(ts_list)
    for i in range(len(ts_list)):
        if coll2.find({'ts': ts_list[len(ts_list) - 1 - i]}).count() > 0:
            flag_list[i] = 0
    if flag_list == [0] * len(ts_list):
        print('no new data!')
        return

    conf = SparkConf().setAppName("tempPredict").setMaster("local[4]")
    #        .set("spark.ui.port", "4050")\
    sc = SparkContext(conf=conf)

    # 生成各设备前24个小时内的温度列表(需要24小时数据,判断需要选择的模型)
    did_ts_temp_list = list(coll.find({'temp': {'$gt': 0}, 'ts': {'$gte': t26HourBefore, '$lte': nowTime}},
                                      {'ts': 1, 'did': 1, 'temp': 1, '_id': 0}).sort('ts'))

    rdd1 = sc.parallelize(did_ts_temp_list, 3) \
        .groupBy(lambda x: x['did']) \
        .map(preprocess1)

    # 将各设备的温度列表导入模型，预测未来的温度
    # rdd1.foreach(tempPredt)
    rdd1.foreachPartition(tempPredt)

    connection.close()
    sc.stop()


if __name__ == "__main__":
    main()
