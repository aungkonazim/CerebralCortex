# Copyright (c) 2017, MD2K Center of Excellence
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
import numpy as np
from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.datastream import DataStream


def analyze_events_with_features(features):
    ecg_feature_stream_array = features[1][0][0]
    rip_feature_stream_array = features[1][0][1]
    accel_feature_stream_array = features[1][0][2]
    final_feature_stream_array = ecg_feature_stream_array + rip_feature_stream_array + accel_feature_stream_array

    stress_ground_truth_stream = features[1][1]

    stress_marks = stress_ground_truth_stream.data

    featureLabels = []
    finalFeatures = []
    subjects = []

    startTimes = {}

    for dp in stress_marks:
        if dp.sample[0][:2] == 'c4':
            if features[0] not in startTimes:
                startTimes[features[0]] = np.inf
            startTimes[features[0]] = min(startTimes[features[0]], dp.start_time.timestamp())


    feature_matrix = [None]*len(final_feature_stream_array[0].data)

    for i in range(len(final_feature_stream_array[0].data)):
        feature_matrix[i]=[]
        for j in range(len(final_feature_stream_array[0:11])):
            feature_matrix[i].append(final_feature_stream_array[j].data[i].sample)
        feature_matrix[i]=(final_feature_stream_array[0].data[i].start_time.timestamp(), feature_matrix[i])





    # for line in features:
    #     id = line[0]
    #     ts = line[1]
    #     f = line[2:]
    #
    #     if ts < startTimes[id]:
    #         continue  # Outside of starting time
    #
    #     label = checkStressMark(stress_marks, id, ts)
    #     if len(label) > 0:
    #         stressClass = decodeLabel(label[0][0])
    #
    #         featureLabels.append(stressClass)
    #         finalFeatures.append(f)
    #         subjects.append(id)
    #
    # return finalFeatures, featureLabels, subjects