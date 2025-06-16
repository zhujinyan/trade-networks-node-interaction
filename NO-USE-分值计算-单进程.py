import numpy as np
from itertools import combinations, permutations
import csv


def get_order(re_index, sub_count):
    order_list = []

    for i in combinations(re_index, sub_count):
        all_sorted = list(permutations(i))
        for item in all_sorted:
            order_list.append(list(item))

    return order_list


def attack(network, target, attacker, order, in_out_ratio, cal_infl, attack_rate):
    cal_infl, exp_wo_att = attack_helper(network, target, order, in_out_ratio,
                                         cal_infl, attack_rate)
    exp_wiz_att = 0

    for i in range(len(order) + 1):
        order.insert(i, attacker)
        cal_infl, add = attack_helper(network, target, order, in_out_ratio,
                                      cal_infl, attack_rate)
        exp_wiz_att += add
        order.remove(attacker)

    exp_wiz_att /= (len(order) + 1)

    return cal_infl, exp_wiz_att - exp_wo_att


def attack_helper(network, target, order, in_out_ratio, cal_infl, attack_rate):
    key = ''
    for o in order:
        key += str(o) + '_'

    if key in cal_infl:
        return cal_infl, cal_infl[key]

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

            reduce_rate = reduced_net.sum(axis=0)[k] / net.sum(axis=0)[k] * in_out_ratio[k]

            for j in range(row):  # spread process
                reduced_net[k][j] += net[k][j] * reduce_rate

            spread_order.remove(k)

        net = net - reduced_net
        results.append(results[-1] - reduced_net.sum(axis=0)[target])

    while len(results) != row:
        results.append(results[-1])

    cal_infl[key] = results[0] - results[-1]

    return cal_infl, cal_infl[key]


def sim_attack(network, target, attacker, s_size, in_out_ratio, cal_infl, attack_rate=0.1):
    re_index = list(range(len(network)))
    re_index.remove(target)
    re_index.remove(attacker)

    orders = get_order(re_index, s_size)

    influence = []
    for order in orders:
        cal_infl, add = attack(network, target, attacker, order, in_out_ratio,
                              cal_infl, attack_rate)
        influence.append(add)

    with open('results.csv', 'a', encoding='utf-8', newline='') as file:
        csvwriter = csv.writer(file)
        csvwriter.writerow([s_size, target, attacker, np.mean(influence)])


if __name__ == '__main__':
    with open('results.csv', 'w', encoding='utf-8', newline='') as file:
        csvwriter = csv.writer(file)
        csvwriter.writerow(['s_size', 'target', 'attacker', 'influence'])

    network = [[0, 1, 0], [0, 0, 0], [0, 1, 0]]    # 网络是一个二维矩阵
    target = 1     # 目标节点的编号
    attacker = 0   # 攻击节点的编号
    s_size = 1      # 攻击节点集合大小, 建议取1
    in_out_ratio = [1, 1, 1]   # 按照节点编号顺序, 记录每个节点的 入度/出度 比
    attack_rate = 0.1   # 攻击率

    cal_infl = {}
    sim_attack(network, target, attacker, s_size, in_out_ratio, cal_infl, attack_rate)