import numpy as np
import pymssql
import matplotlib.pyplot as plt
import os
import pandas as pd
import csv

'''
    Use cal-network-matrix to calculate all network matrice that are covered
    Choose a matrix, order all the countries based on the corresonding value
    Then perform attack
'''

regions = ["usa", "china", "japan", "germany", "unitedkingdom", "india", "france", "italy", "canada", "korea","russia",
            "brazil", "australia", "spain", "mexico", "indonesia", "netherlands", "saudiarabia", "turkey", "switzerland","poland",
            "sweden", "belgium", "thailand", "ireland", "argentina", "norway", "israel", "austria", "nigeria", "southafrica",
            "bangladesh", "egypt", "denmark", "singapore", "philippines", "malaysia", "hongkong", "vietnam", "unitedarab", "pakistan",
            "chile","colombia","finland", "romania", "czechia", "newzealand", "portugal", "iran", "peru"]
regions = sorted(regions)


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


def get_order(hs, time, target, indicator):
    conn, cur = connect()

    cur.execute('''
            select region, %s from UN_multiple_indicator where hs = %d and time = %d and binomial_cutdown = 0
        ''' % (indicator, hs, time))
    rows = cur.fetchall()
    rows = sorted(rows, key=lambda row: row[1], reverse=True)

    region_order = [regions.index(row[0]) for row in rows if row[0] != target]

    return region_order


def read_csv(path, hs, time, target):
    df = pd.read_csv(os.path.join(path, f'{hs}_results.csv'))

    data = df[(df['time'] == time) & (df['target'] == target)]
    y = [regions.index(t) for t in data.sort_values('influence', ascending=False)['attacker']]
    auc = [regions.index(t) for t in data.sort_values('auc', ascending=False)['attacker']]

    return y, auc


def attack(network, target, order, attack_rate):
    net = np.array(network)
    row = int(net.shape[0])
    column = int(net.shape[1])

    results = []
    results.append(net.sum(axis=0)[target])  # initial input of target

    # handled = []    # record nodes which have been handled
    for node in order:
        # handled.append(node)
        reduced_net = np.zeros((row, column))  # record reduced count for net

        for i in range(row):
            # if i in handled:
            #     continue
            reduced_net[node][i] += net[node][i] * attack_rate

        spread_order = order[:]
        # for n in handled:
        #     spread_order.remove(n)
        spread_order.remove(node)
        while len(spread_order) != 0:
            for k in spread_order:  # choose the next node
                if reduced_net.sum(axis=0)[k] != 0:
                    break

            if k == spread_order[-1] and reduced_net.sum(axis=0)[k] == 0:
                break

            reduce_rate = reduced_net.sum(axis=0)[k] / net.sum(axis=0)[k]

            for j in range(row):  # spread process
                # if j in handled:
                #     continue
                reduced_net[k][j] += net[k][j] * reduce_rate

            spread_order.remove(k)

        net = net - reduced_net
        results.append(results[-1] - reduced_net.sum(axis=0)[target])

    while len(results) != row:
        results.append(results[-1])

    return results


def figure(hs, time, indicator, results, result_y):
    plt.plot(range(50), result_y[0], label='subcount1')
    plt.plot(range(50), result_y[1], label='subcount2')
    plt.plot(range(50), result_y[2], label='subcount3')
    plt.plot(range(50), results, label=indicator)
    # plt.scatter(range(50), results)
    plt.legend()

    if not os.path.exists(str(hs)):
        os.mkdir(str(hs))
    if not os.path.exists(os.path.join(str(hs), str(time))):
        os.mkdir(os.path.join(str(hs), str(time)))
    plt.savefig(os.path.join(str(hs), str(time), indicator+'.png'))
    plt.close()


def main():
    indicators = ['degree', 'in_degree', 'out_degree', 'clustering', 'clustering_weight', 'core_number', 'degree_centrality',
                  'in_degree_centrality', 'out_degree_centrality', 'eigenvector_centrality', 'eigenvector_centrality_wight',
                  'second_order_centrality', 'betweenness_centrality', 'betweenness_centrality_distance', 'closeness_centrality',
                  'closeness_centrality_distance', 'degree_weight_Di', 'degree_weight', 'pagerank', 'in_largest_cc', 'd_unit', 'd_diff_in',
                  'd_diff_out', 'd_diff_both', 'shs_constraint', 'shs_constraint_weight', 'shs_effective_size', 'shs_effective_size_weight',
                  'hits_0', 'hits_1', 'd_an_o_o_None', 'd_an_o_i_None', 'd_an_o_io_None', 'd_an_i_o_None', 'd_an_i_i_None',
                  'd_an_i_io_None', 'd_an_io_o_None', 'd_an_io_i_None', 'd_an_io_io_None', 'd_an_o_o_weight', 'd_an_o_i_weight',
                  'd_an_o_io_weight', 'd_an_i_o_weight', 'd_an_i_i_weight', 'd_an_i_io_weight', 'd_an_io_o_weight', 'd_an_io_i_weight',
                  'd_an_io_io_weight', 'closeness_vitality', 'closeness_vitality_weight', 'community_louvain_communities',
                  'community_greedy_modularity_communities', 'community_girvan_newman', 'c_flu_2', 'c_flu_3', 'c_flu_4', 'c_flu_5',
                  'c_flu_6', 'c_flu_7', 'c_flu_8', 'c_flu_9', 'community_kernighan_lin_bisection', 'community_asyn_lpa_communities',
                  'community_label_propagation_communities']

    target = 'china'
    attack_rate = 0.1
    hs = 87
    time = 201001

    network = read_net(hs, time)

    result_y = []
    result_auc = []
    for i in range(1, 4):
        folder_path = os.path.join('influ&auc', f'subcount_{i}')
        y, auc = read_csv(folder_path, hs, time, target)
        result_y.append(attack(network, regions.index(target), y, attack_rate))
        result_auc.append(attack(network, regions.index(target), auc, attack_rate))

    with open('results.csv', 'a', encoding='utf-8', newline='') as f:
        csvwriter = csv.writer(f)
        for i, re in enumerate(result_y):
            csvwriter.writerow([f'subcount_{i+1}'] + re)

    for indicator in indicators:
        order = get_order(hs, time, target, indicator)

        results = attack(network, regions.index(target), order, attack_rate)

        # with open('result.txt', 'a', encoding='utf-8') as file:
        #     file.write(str(hs) + ' ' + str(time) + ' ' + indicator + ' ' + str(results) + '\n')
        #
        # figure(hs, time, indicator, results, result_y)

        with open('results.csv', 'a', encoding='utf-8', newline='') as f:
            csvwriter = csv.writer(f)
            csvwriter.writerow([indicator] + results)



if __name__ == '__main__':
    main()





