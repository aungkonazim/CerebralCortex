# Copyright (c) 2016, MD2K Center of Excellence
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import gzip
import time
import uuid


import os
from cerebralcortex.data_processor.preprocessor import parser
from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.datastream import DataStream

def readfile(filename):
    data = []
    with open(filename, 'rt') as f:
        count = 0
        for l in f:
            dp = parser.data_processor_ppg(l)
            if isinstance(dp, DataPoint):
                data.append(dp)
                count += 1
    return data

def readfile_ground_truth(filename):
    data = []
    with open(filename, 'rt') as f:
        count = 0
        for l in f:
            if count != 0:
                dp = parser.ground_truth_data_processor(l)
                if isinstance(dp, DataPoint):
                    data.append(dp)
                count+=1
    return data

def loader(path: str):
    participant = path.split('\\')[-2]
    participant_uuid = uuid.uuid4()

    try:
        ppg_left = DataStream(None, participant_uuid)
        ppg_left.data = readfile(path+'left_wrist_prob.csv')

        ppg_right = DataStream(None, participant_uuid)
        ppg_right.data = readfile(path+'right_wrist_prob.csv')

        stress_marks = DataStream(None, participant_uuid)
        stress_marks.data = readfile_ground_truth(path+'label.csv')

        return {"participant": participant, "ppg_left":ppg_left, "ppg_right": ppg_right,"stress_marks": stress_marks}

    except Exception as e:
        print("File missing for %s" % participant)
        return {"ERROR": 'missing data file'}




start_time = time.time()
path_collection = []
path = 'C:\\Users\\aungkon\\Desktop\\model\\data'
participant_dir = list(os.listdir(path))
for participant in participant_dir:
    count = 0
    path_to_date = path + '\\'+str(participant)
    date_dir = list(filter(lambda x: x[0] == '2',os.listdir(path_to_date)))
    for date in date_dir:
        path_to_csv = path_to_date + '\\'+str(date)+ '\\'
        path_collection.append(path_to_csv)


from pyspark import SparkContext
sc = SparkContext("local", "Simple App")

ids = sc.parallelize(path_collection[0])

data = ids.map(lambda i: loader(i))

print(data.collect())
#
# cstress_feature_vector = cStress(data)
#
# features = cstress_feature_vector.collect()
#
# features2 = copy.deepcopy(features)
#
# cstress_model = cstress_model(features=features2)
#
# # results = ids.map(loader)
# # pprint(results.collect())
# end_time = time.time()
# print(end_time - start_time)
