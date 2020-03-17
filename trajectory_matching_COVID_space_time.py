# coding:utf-8

"""
    input: test sample trajectory.txt and confirmed patient datadase.txt
    (number start_longitude start_latitude start_time end_longitude end_latitude end_time)
    output：risk of infection(0-1) and risk trajectory（top-5）
"""

from pyspark import SparkContext, SparkConf
import os
import math
import linecache

# the space and time range of the trajectories
box = [125, 44, -24*10, 131, 47, 24 * 30]  # range of data: 1.13-1.23-2.11 24*20
# the segments are partitioned into nx*ny*nt segments
nx = 6/0.002
ny = 3/0.002
nt = 40
var_s = 1250/math.log(2)
var_t = 50/math.log(2)

xmin, ymin, tmin, xmax, ymax, tmax = box
wx = (xmax - xmin) / nx
wy = (ymax - ymin) / ny
wt = (tmax - tmin) / nt
# the radius of space-time buffer
distance_threshold = 200

class trajectory_match(object):
    def __init__(self, train_jwd, test_jwd, train, test, jdk):
        os.environ['JAVA_HOME'] = jdk
        self.train_jwd = train_jwd
        self.test_jwd = test_jwd
        self.train_ori = train
        self.test_ori = test

    def line2pair(self, line):
        l = line.split(' ')
        num = int(l[0])
        id = int(l[1])
        seg = list(map(float, l[2:]))
        return num, id, seg

    # calculate the distance according to lon,lat
    def haversine(self, lon1, lat1, lon2, lat2):  # _xa0, _ya0, _xb0, _yb0
        lon1 = math.radians(lon1)
        lat1 = math.radians(lat1)
        lon2 = math.radians(lon2)
        lat2 = math.radians(lat2)
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        c = 2 * math.asin(math.sqrt(a))
        r = 6371
        return c * r * 1000

    def _preprocess(self, seg):
        x0, y0, t0, x1, y1, t1 = seg
        _x0 = (x0 - xmin) / wx
        _y0 = (y0 - ymin) / wy
        _t0 = (t0 - tmin) / wt
        _x1 = (x1 - xmin) / wx
        _y1 = (y1 - ymin) / wy
        _t1 = (t1 - tmin) / wt

        return _x0, _y0, _t0, _x1, _y1, _t1

    def _get_point(self, seg, delta):
        x0, y0, t0, x1, y1, t1 = seg
        return [x0 * (1 - delta) + x1 * delta,
                y0 * (1 - delta) + y1 * delta,
                t0 * (1 - delta) + t1 * delta]

    # get the space-time cells that intersects with the segment
    def intersect_boxes(self, seg):
        _x0, _y0, _t0, _x1, _y1, _t1 = self._preprocess(seg)
        # print(_x0, _y0, _t0, _x1, _y1, _t1)
        delta_list = []
        x_inc = 0
        if _x0 < _x1:
            x_inc = 1
            _dx = _x1 - _x0
            for _x in range(math.floor(_x0) + 1, math.ceil(_x1)):
                delta = (_x - _x0) / _dx
                # print(delta)
                delta_list.append((delta, 'x'))

        elif _x0 > _x1:
            x_inc = -1
            _dx = _x0 - _x1
            for _x in range(math.floor(_x1) + 1, math.ceil(_x0)):
                delta = 1 - (_x - _x1) / _dx
                delta_list.append((delta, 'x'))

        y_inc = 0
        if _y0 < _y1:
            y_inc = 1
            _dy = _y1 - _y0
            for _y in range(math.floor(_y0) + 1, math.ceil(_y1)):
                delta = (_y - _y0) / _dy
                delta_list.append((delta, 'y'))

        elif _y0 > _y1:
            y_inc = -1
            _dy = _y0 - _y1
            for _y in range(math.floor(_y1) + 1, math.ceil(_y0)):
                delta = 1 - (_y - _y1) / _dy
                delta_list.append((delta, 'y'))

        t_inc = 0
        if _t0 < _t1:
            t_inc = 1
            _dt = _t1 - _t0
            for _t in range(math.floor(_t0) + 1, math.ceil(_t1)):
                delta = (_t - _t0) / _dt
                delta_list.append((delta, 't'))

        elif _t0 > _t1:
            t_inc = -1
            _dt = _t0 - _t1
            for _t in range(math.floor(_t1) + 1, math.ceil(_t0)):
                delta = 1 - (_t - _t1) / _dt
                delta_list.append((delta, 't'))

        delta_list = list(sorted(delta_list))

        c_x = math.floor(_x0)
        c_y = math.floor(_y0)
        c_t = math.floor(_t0)
        cells = [(c_x, c_y, c_t)]
        for delta in delta_list:
            if delta[1] == 'x':
                c_x += x_inc
            elif delta[1] == 'y':
                c_y += y_inc
            else:
                c_t += t_inc
            cells.append((c_x, c_y, c_t))
        # print('cells:', cells)
        return cells

    # intersected partition
    def flat_idseg(self, pair):
        num, tra_id, seg = pair
        boxid_idsseg = []
        # print((tra_id, seg))
        for box_id in self.intersect_boxes(seg):
            boxid_idsseg.append((box_id, (tra_id, seg, num)))
        return boxid_idsseg

    # buffered partiotion
    def flat_idseg_with_bounds(self, pair):
        num, tra_id, seg = pair
        boxid_idsseg = []
        boxes = set()
        for box_id in self.intersect_boxes(seg):
            boxes.update(self.near_box(box_id))
        # print('boxes:', boxes)
        for box_id in boxes:
            boxid_idsseg.append((box_id, (tra_id, seg, num)))
        return boxid_idsseg

    def near_box(self, box_id):
        x, y, t = box_id
        for delta_x in [-1, 0, 1]:
            for delta_y in [-1, 0, 1]:
                yield x + delta_x, y + delta_y, t

    def _distance_func(self, distance):  # unit: meter
        if distance > 200:  # distance_threshold
            return 0
        elif distance <= 100:
            return 1
        else:
            return 1 - (distance - 100) / 100

    def score(self, box_id, seg_a, seg_b):
        xa0, ya0, ta0 = seg_a[0], seg_a[1], seg_a[2]
        xa1, ya1, ta1 = seg_a[3], seg_a[4], seg_a[5]
        xb0, yb0, tb0 = seg_b[0], seg_b[1], seg_b[2]
        xb1, yb1, tb1 = seg_b[3], seg_b[4], seg_b[5]
        da0b0 = self.haversine(xa0, ya0, xb0, yb0)
        da0b1 = self.haversine(xa0, ya0, xb1, yb1)
        da1b0 = self.haversine(xa1, ya1, xb0, yb0)
        da1b1 = self.haversine(xa1, ya1, xb1, yb1)
        d_list = [da0b0, da0b1, da1b0, da1b1]
        _t0 = max(ta0, tb0)  # Intersection of two paths
        _t1 = min(ta1, tb1)
        if ta1 < tb0:  # the test sample arrived before confirmed patients
            return 0

        _xa0, _ya0, _ta0 = self._get_point(seg_a, (_t0 - ta0) / (ta1 - ta0))
        _xa1, _ya1, _ta1 = self._get_point(seg_a, (_t1 - ta0) / (ta1 - ta0))
        _xb0, _yb0, _tb0 = self._get_point(seg_b, (_t0 - tb0) / (tb1 - tb0))
        _xb1, _yb1, _tb1 = self._get_point(seg_b, (_t1 - tb0) / (tb1 - tb0))
        _d0 = self.haversine(_xa0, _ya0, _xb0, _yb0)
        _d1 = self.haversine(_xa1, _ya1, _xb1, _yb1)

        # min
        d = min(d_list)
        d_index = d_list.index(min(d_list))
        # print(d)
        if d <= distance_threshold:
            # print(d_index)
            score_space = math.exp(-d * d / (2 * var_s))
            if d_index == 0:
                if self.in_box(xb0, yb0, tb0, box_id):
                    # a0b0
                    if _d0 == 0 and _d1 == 0:
                        if ta0 >= tb1:  # the time did not cross
                            t = abs(ta0 - tb1)
                        else:  # the time crossed
                            # print('_d0=0,_d1=0 stay together for a long time')
                            return 1
                    else:
                        t = abs(ta0 - tb0)
                    score_time = math.exp(-t * t / (2 * var_t))
                    return score_space * score_time
                else:
                    return 0
            elif d_index == 1:
                if self.in_box(xb1, yb1, tb1, box_id):
                    # a0b1
                    if _d0 == 0 and _d1 == 0:
                        # print('_d0=0,_d1=0 stay together for a long time')
                        return 1
                    t = abs(ta0 - tb1)
                    score_time = math.exp(-t * t / (2 * var_t))
                    return score_space * score_time
                else:
                    return 0
            elif d_index == 2:
                if self.in_box(xb0, yb0, tb0, box_id):
                    # a1b0
                    if _d0 == 0 and _d1 == 0:
                        # print('_d0=0,_d1=0 stay together for a long time')
                        return 1
                    t = abs(ta1 - tb0)
                    score_time = math.exp(-t * t / (2 * var_t))
                    return score_space * score_time
                else:
                    return 0
            else:
                if self.in_box(xb1, yb1, tb1, box_id):
                    # a1b1
                    if _d0 == 0 and _d1 == 0:  #
                        # print('_d0=0,_d1=0 stay together for a long time')
                        return 1
                    t = abs(ta1 - tb1)
                    score_time = math.exp(-t * t / (2 * var_t))
                    return score_space * score_time
                else:
                    return 0

        return 0

    def in_box(self, x, y, t, box_id):
        _x0 = math.floor((x - xmin) / wx)
        _y0 = math.floor((y - ymin) / wy)
        _t0 = math.floor((t - tmin) / wt)
        return (_x0, _y0, _t0) == box_id

    def get_scores(self, joined_pair):
        """
        :param joined_pair:(box_id,([(tra_id,[xyt]),...],[(tra_id,[xyt])...]))
        :return:
        """
        boxid, pair = joined_pair
        mo_list, car_list = pair

        result = []

        for mo in mo_list:
            moid, moseg, monum = mo
            for car in car_list:
                carid, carseg, carnum = car
                if moid == carid:
                    continue
                tmp_score = self.score(boxid, moseg, carseg)
                if tmp_score >= 0.0000001:
                    result.append(((moid, carid), tmp_score, (moseg, carseg), (monum, carnum)))
        return result

    def get_line_context(self, file_path, line_number):
        # file = open(file_path)
        # data = file.read().encode("utf8")
        return linecache.getline(file_path, line_number).strip("\n").split()

    def match(self):
        # two sample datasets
        tra_file1 = self.test_jwd  # test
        tra_file2 = self.train_jwd  # confirmed patient database
        conf = SparkConf().setAppName('trajectory_matching').setMaster('local') \
            .set('spark.executor.memokongjianry', '12g') \
            .set('spark.driver.memory', '2g')
        sc = SparkContext(conf=conf)

        # [(id,seg),...]
        test_rdd = sc.textFile(tra_file1).map(
            self.line2pair)  # from data1
        COVID_rdd = sc.textFile(tra_file2).map(self.line2pair)  # from data2

        test_box_seg_rdd = test_rdd.flatMap(self.flat_idseg_with_bounds)
        COVID_box_seg_rdd = COVID_rdd.flatMap(self.flat_idseg)

        # combined by Key
        grouped_test_box_seg_rdd = test_box_seg_rdd.groupByKey()
        grouped_COVID_box_seg_rdd = COVID_box_seg_rdd.groupByKey()

        joined_rdd = grouped_test_box_seg_rdd.join(grouped_COVID_box_seg_rdd)
        scores_rdd = joined_rdd.flatMap(self.get_scores)  # calculate the score of two trajectories
        # sort scores by value
        ordered_idid_score_seg_rdd = scores_rdd.sortBy(lambda x: x[1], False)
        # print(ordered_idid_score_seg_rdd.collect())

        # get the trajectory of value=1
        # sc = sc.addPyFile("trajectory_matching_COVID_space_time_gauss_v2.py")
        top5_rdd = ordered_idid_score_seg_rdd.filter(lambda x: x[1] == 1).collect()
        if len(top5_rdd) < 5:
            top5_rdd = ordered_idid_score_seg_rdd.take(5)

        filename_test = self.test_ori
        filename_COVID = self.train_ori
        results = []
        if top5_rdd:  # if top_rdd is not null
            length = len(top5_rdd)
            for i in range(length):
                test_tra = self.get_line_context(filename_test, top5_rdd[i][3][0])
                COVID_tra = self.get_line_context(filename_COVID, top5_rdd[i][3][1])
                result = [top5_rdd[i][0], top5_rdd[i][1], test_tra[2:], COVID_tra[2:]]
                results.append(result)
            value = top5_rdd[0][1]
        else:
            value = 0

        return value, results
