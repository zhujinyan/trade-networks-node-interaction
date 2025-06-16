import numpy as np
import pymssql
import matplotlib.pyplot as plt
import os
from itertools import combinations, permutations
import csv
import time

'''
    target = china, attacker = usa, watch influence of attacker to target
'''


regions = ["usa", "china", "japan", "germany", "unitedkingdom", "india", "france", "italy", "canada", "korea","russia",
            "brazil", "australia", "spain", "mexico", "indonesia", "netherlands", "saudiarabia", "turkey", "switzerland","poland",
            "sweden", "belgium", "thailand", "ireland", "argentina", "norway", "israel", "austria", "nigeria", "southafrica",
            "bangladesh", "egypt", "denmark", "singapore", "philippines", "malaysia", "hongkong", "vietnam", "unitedarab", "pakistan",
            "chile","colombia","finland", "romania", "czechia", "newzealand", "portugal", "iran", "peru"]
regions = sorted(regions)

in_to_out_ratio_85 = [0.79331765, 1.000487878, 1.09848839, 1.645707087, 0.544437664, 0.511521329, 0.964567504,
                      0.388282961, 0.732254415, 0.295483975, 0.67228078, 0.752260619, 1.116772598, 1.290968097,
                      1.007264197, 0.876965071, 0.479789229, 1.268711982, 0.397504891, -0.34766433, 1.209029989,
                      1.351206284, 0.861916075, 0.926693839, 1.72993915, 1.084837463, 0.543263792, 0.821433802,
                      0.513807392, 0.043045255, 0.720403504, -3.108364281, 1.441838736, 0.878195096, 0.925371665,
                      0.264452504, 0.845301044, 0.118626279, 1.123945695, 0.812330425, -0.428504603, 1.004682301,
                      0.165416913, 0.90442725, 0.62864192, 0.346765852, 2.705022027, 0.643919139, 0.539147664,
                      0.919741062]

def connect():
    conn = pymssql.connect(host='10.51.86.201', user="ZhuJinyan", password="Zhu2022Jinyan", database="Project")
    cur = conn.cursor()
    if not cur:
        raise (NameError, "Connection failed.")
    else:
        return conn, cur

def read_net(hs, time):
    conn, cur = connect()

    cur.execute('''
        select * from hs2_adjacency_matrix where hs = %d and period = %d
    ''' % (hs, time))
    rows = cur.fetchall()

    network = [[] * len(rows)] * len(rows)
    for row in rows:
        network[regions.index(row[2])] = row[3:]

    return network


def get_order(re_index, sub_count):
    order_list = []

    for i in combinations(re_index, sub_count + 1):
        all_sorted = list(permutations(i))
        for item in all_sorted:
            order_list.append(list(item))

    return order_list


def attack(network, target, attacker, order, cal_infl, cal_auc, attack_rate=0.1):
    cal_infl, cal_auc, exp_wo_att, auc_wo_att = attack_helper(network, target, attacker, order, cal_infl, cal_auc)
    exp_wiz_att = 0
    auc_wiz_att = 0

    for i in range(len(order) + 1):
        order.insert(i, attacker)
        cal_infl, cal_auc, add, add_auc = attack_helper(network, target, attacker, order, cal_infl, cal_auc, attack_rate)
        exp_wiz_att += add
        auc_wiz_att += add_auc
        order.remove(attacker)

    exp_wiz_att /= (len(order) + 1)
    auc_wiz_att /= (len(order) + 1)

    return cal_infl, cal_auc, exp_wiz_att - exp_wo_att, auc_wiz_att - auc_wo_att


def attack_helper(network, target, attacker, order, cal_infl, cal_auc, attack_rate=0.1):
    key = ''
    for o in order:
        key += str(o) + '_'

    if key in cal_infl:
        return cal_infl, cal_auc, cal_infl[key], cal_auc[key]

    net = np.array(network)
    row = int(net.shape[0])
    column = int(net.shape[1])

    results = []
    results.append(net.sum(axis=0)[target])  # initial input of target

    for node in order:
        reduced_net = np.zeros((row, column))  # record reduced count for net

        for i in range(row):
            reduced_net[node][i] += net[node][i] * attack_rate

        spread_order = order[:]
        spread_order.remove(node)
        while len(spread_order) != 0:
            for k in spread_order:  # choose the next node
                if reduced_net.sum(axis=0)[k] != 0:
                    break

            if k == spread_order[-1] and reduced_net.sum(axis=0)[k] == 0:
                break

            reduce_rate = reduced_net.sum(axis=0)[k] / net.sum(axis=0)[k] * in_to_out_ratio_85[k]

            for j in range(row):  # spread process
                reduced_net[k][j] += net[k][j] * reduce_rate
            reduced_net[attacker][target] = 0

            spread_order.remove(k)

        net = net - reduced_net
        results.append(results[-1] - reduced_net.sum(axis=0)[target])

    while len(results) != row:
        results.append(results[-1])

    cal_infl[key] = results[0] - results[-1]
    cal_auc[key] = sum(results) - 0.5 * (results[0] + results[-1])

    return cal_infl, cal_auc, cal_infl[key], cal_auc[key]


def figure(time, indicator, results):
    plt.plot(range(50), results)
    plt.scatter(range(50), results)

    if not os.path.exists(str(time)):
        os.mkdir(str(time))
    plt.savefig(os.path.join(str(time), indicator+'.png'))


def main():
    pass

if __name__ == '__main__':
    if not os.path.exists('influ&auc'):
        os.mkdir('influ&auc')

    for sub_count in range(1, 2):
        start = time.time()

        if not os.path.exists(os.path.join('influ&auc', f'subcount_{sub_count}')):
            os.mkdir(os.path.join('influ&auc', f'subcount_{sub_count}'))

        hs = 87
        # period = 201001
        target = 'china'

        attack_rate = 0.1

        with open(os.path.join('influ&auc', f'subcount_{sub_count}', f'indirect_attrate{attack_rate}_'
                     f'{hs}_results.csv'), 'w', encoding='utf-8', newline='') as file:
            csvwriter = csv.writer(file)
            csvwriter.writerow(['time', 'sub_count', 'target', 'attacker', 'influence'])

        # for hs in range(87, 88):
        #     if hs == 77 or hs == 98:
        #         continue

        for year in range(2010, 2022):
            for month in range(1, 13):
                period = year * 100 + month

                # for target in regions:
                #     if target == 'china':
                #         continue

                cal_infl = {}
                cal_auc = {}
                for attacker in regions:
                    if attacker == target:
                        continue

                    # print(attacker)

                    tar_index = regions.index(target)
                    att_index = regions.index(attacker)
                    re_index = [regions.index(x) for x in regions]
                    re_index.remove(tar_index)
                    re_index.remove(att_index)

                    orders = get_order(re_index, sub_count)

                    network = read_net(hs, period)

                    influence = []
                    auc = []
                    for order in orders:
                        cal_infl, cal_auc, add, add_auc = attack(network, tar_index, att_index,
                                                                 order, cal_infl, cal_auc, attack_rate)
                        influence.append(add)
                        auc.append(add_auc)


                    with open(os.path.join('influ&auc', f'subcount_{sub_count}', f'indirect_attrate{attack_rate}_'
                     f'{hs}_results.csv'), 'a', encoding='utf-8', newline='') as file:
                        csvwriter = csv.writer(file)
                        csvwriter.writerow([period, sub_count, regions[tar_index],
                                            regions[att_index], np.mean(influence)])

                with open('time.txt', 'a', encoding='utf-8') as f:
                    f.write(f'攻击节点集合大小: {sub_count}, 耗时: {time.time()-start}\n')
