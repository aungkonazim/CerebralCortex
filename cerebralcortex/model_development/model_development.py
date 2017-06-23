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
from collections import Counter


def decode_label(label):
    label = label[:2]  # Only the first 2 characters designate the label code
    mapping = {'c1': 0, 'c2': 1, 'c3': 1, 'c4': 0, 'c5': 0, 'c6': 0, 'c7': 2, }
    return mapping[label]


def check_stress_mark(stress_mark, start_time):
    endtime = start_time + 60  # One minute windows
    result = []
    for dp in stress_mark:
        st = dp.start_time.timestamp()
        et = dp.end_time.timestamp()
        gt = dp.sample[0][:2]
        if gt not in ['c7']:
            if (start_time > st) and (endtime < et):
                result.append(gt)
    data = Counter(result)
    return data.most_common(1)


def analyze_events_with_features(features):
    ecg_feature_stream_array = features[1][0][0]
    rip_feature_stream_array = features[1][0][1]
    accel_feature_stream_array = features[1][0][2]
    final_feature_stream_array = ecg_feature_stream_array + rip_feature_stream_array + accel_feature_stream_array

    stress_ground_truth_stream = features[1][1]
    stress_marks = stress_ground_truth_stream.data

    start_times = {}

    for dp in stress_marks:
        if dp.sample[0][:2] == 'c4':
            if features[0] not in start_times:
                start_times[features[0]] = np.inf
            start_times[features[0]] = min(start_times[features[0]], dp.start_time.timestamp())
    feature_matrix = [None]*len(final_feature_stream_array[0].data)

    for i in range(len(final_feature_stream_array[0].data)):
        feature_matrix[i] = []
        for j in range(len(final_feature_stream_array[0:11])):
            feature_matrix[i].append(final_feature_stream_array[j].data[i].sample)
        feature_matrix[i] = (final_feature_stream_array[0].data[i].start_time.timestamp(), feature_matrix[i])

    feature_labels = []
    final_features = []
    subjects = []

    pid = features[0]
    for feature_vector in feature_matrix:
        ts = feature_vector[0]
        f = feature_vector[1]
        #
        # if ts < start_times[pid]:
        #     continue  # Outside of starting time

        label = check_stress_mark(stress_marks, ts)

        if len(label) > 0:
            stress_class = decode_label(label[0][0])

            feature_labels.append(stress_class)
            final_features.append(f)
            subjects.append(pid)

    return final_features, feature_labels, subjects

