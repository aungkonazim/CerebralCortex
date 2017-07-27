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

import argparse
import gzip
import time
import uuid
from pprint import pprint

import os

from cerebralcortex.CerebralCortex import CerebralCortex
from cerebralcortex.data_processor.cStress import cStress
from cerebralcortex.data_processor.preprocessor import parser
from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.datastream import DataStream
from cerebralcortex.legacy import find
from pyspark import SparkContext



argparser = argparse.ArgumentParser(description="Cerebral Cortex Test Application")
argparser.add_argument('--base_directory')
args = argparser.parse_args()

# To run this program, please specific a program argument for base_directory that is the path to the test data files.
# e.g. --base_directory /Users/hnat/data/
basedir = args.base_directory

configuration_file = os.path.join(os.path.dirname(__file__), 'cerebralcortex.yml')

# CC = CerebralCortex(configuration_file, master="local[*]", name="Memphis cStress Development App")


def readfile(filename):
    data = []
    with gzip.open(filename, 'rt') as f:
        count = 0
        for l in f:
            dp = parser.data_processor(l)
            if isinstance(dp, DataPoint):
                data.append(dp)
                count += 1
            if count > 20000:
                break
    return data

def readfile_ground_truth(filename):
    data = []
    with gzip.open(filename, 'rt') as f:
        count = 0
        for l in f:
            dp = parser.ground_truth_data_processor(l)
            if isinstance(dp, DataPoint):
                data.append(dp)
                count += 1
            if count > 20000:
                break
    return data

def readfile_ground_truth(filename):
    data = []
    with gzip.open(filename, 'rt') as f:
        count = 0
        for l in f:
            dp = parser.ground_truth_data_processor(l)
            if isinstance(dp, DataPoint):
                data.append(dp)
                count += 1
            if count > 20000:
                break
    return data

def readfile_feature(filename):
    data = []
    with open(filename, 'rt') as f:
        count = 0
        for l in f:
            dp = parser.feature_vector_data_processor(l)
            if isinstance(dp, DataPoint):
                data.append(dp)
                count += 1
    return data



def loader(identifier: int):
    participant = "SI%02d" % identifier

    participant_uuid = uuid.uuid4()

    try:
        ecg = DataStream(None, participant_uuid)
        ecg.data = readfile(basedir+"\\"+participant+"\\"+"ecg.txt.gz")

        rip = DataStream(None, participant_uuid)
        rip.data = readfile(basedir+"\\"+participant+"\\"+"rip.txt.gz")

        accelx = DataStream(None, participant_uuid)
        accelx.data = readfile(basedir+"\\"+participant+"\\"+"accelx.txt.gz")

        accely = DataStream(None, participant_uuid)
        accely.data = readfile(basedir+"\\"+participant+"\\"+"accely.txt.gz")

        accelz = DataStream(None, participant_uuid)
        accelz.data = readfile(basedir+"\\"+participant+"\\"+"accelz.txt.gz")

        stress_marks = DataStream(None, participant_uuid)
        stress_marks.data = readfile_ground_truth(basedir+"\\"+participant+"\\"+"stress_marks.txt.gz")

        feature = DataStream(None, participant_uuid)
        feature.data = readfile_feature("C:\\Users\\aungkon\\Desktop\\Cstress_data\\"+participant+"\\"+"org.md2k.cstress.fv.csv")

        return {"participant": participant, "ecg": ecg, "rip": rip, "accelx": accelx, "accely": accely,
                "accelz": accelz, "stress_marks": stress_marks, "feature" : feature}

    except Exception as e:
        print("File missing for %s" % participant)

        return {"ERROR": 'missing data file'}




start_time = time.time()

sc = SparkContext("local", "Simple App")
ids = sc.parallelize([i for i in range(1,4)])

data = ids.map(lambda i: loader(i)).filter(lambda ds: "participant" in ds)

cstress_feature_vector = cStress(data,sc)

# pprint(cstress_feature_vector.collect())


# results = ids.map(loader)
# pprint(results.collect())
end_time = time.time()
print(end_time - start_time)
