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

from datetime import datetime,timedelta
import pytz

import json
import numpy as np
from collections import Counter
from collections import Sized
from pathlib import Path
from pprint import pprint
from sklearn import svm, metrics, preprocessing
from sklearn.base import clone, is_classifier
from sklearn.cross_validation import LabelKFold
from sklearn.cross_validation import check_cv
from sklearn.externals.joblib import Parallel, delayed
from sklearn.grid_search import GridSearchCV, RandomizedSearchCV, ParameterSampler, ParameterGrid
from sklearn.utils.validation import _num_samples, indexable
from spark_sklearn import GridSearchCV

def model_building(sc,traindata,trainlabels,subjects):
    traindata = np.asarray(traindata, dtype=np.float64)
    trainlabels = np.asarray(trainlabels)

    normalizer = preprocessing.StandardScaler()
    traindata = normalizer.fit_transform(traindata)

    lkf = LabelKFold(subjects, n_folds=len(np.unique(subjects)))

    delta = 0.1
    parameters = {'kernel': ['rbf'],
                  'C': [2 ** x for x in np.arange(-12, 12, 0.5)],
                  'gamma': [2 ** x for x in np.arange(-12, 12, 0.5)],
                  'class_weight': [{0: w, 1: 1 - w} for w in np.arange(0.0, 1.0, delta)]}

    svc = svm.SVC(probability=True, verbose=False, cache_size=2000)
    clf = GridSearchCV(sc=sc,estimator=svc, param_grid=parameters,n_jobs=4,cv=lkf)
    clf.fit(traindata, trainlabels)

    pprint(clf.best_params_)


def decode_label(label):
    label = label[:2]  # Only the first 2 characters designate the label code
    mapping = {'c1': 0, 'c2': 1, 'c3': 1, 'c4': 0, 'c5': 0, 'c6': 0, 'c7': 2, }
    return mapping[label]


def check_stress_mark(stress_mark, start_time):
    endtime = start_time +  timedelta(seconds=60) # One minute windows
    result = []
    for dp in stress_mark:
        st = dp.start_time
        et = dp.end_time
        gt = dp.sample[0][:2]
        if gt not in ['c7']:
            if (start_time > st) and (endtime < et):
                result.append(gt)
    data = Counter(result)
    return data.most_common(1)


def analyze_events_with_features(participant,stress_mark_stream,feature_stream):
    stress_marks = stress_mark_stream.data

    start_times = {}

    for dp in stress_marks:
        if dp.sample[0][:2] == 'c4':
            if participant not in start_times:
                start_times[participant] = datetime.utcnow().replace(tzinfo=pytz.UTC)
            if dp.start_time < start_times[participant]:
                start_times[participant] = dp.start_time
    feature_matrix = feature_stream.data

    feature_labels = []
    final_features = []
    subjects = []


    for feature_vector in feature_matrix:
        ts = feature_vector.start_time
        f = feature_vector.sample

        if ts < start_times[participant]:
            continue  # Outside of starting time

        label = check_stress_mark(stress_marks, ts)

        if len(label) > 0:
            stress_class = decode_label(label[0][0])

            feature_labels.append(stress_class)
            final_features.append(f)
            subjects.append(participant)

    return final_features, feature_labels, subjects








