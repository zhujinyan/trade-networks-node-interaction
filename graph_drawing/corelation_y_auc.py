import pymssql
import pandas as pd
import os
import matplotlib.pyplot as plt
import numpy as np
import csv


regions = ["usa", "china", "japan", "germany", "unitedkingdom", "india", "france", "italy", "canada", "korea","russia",
            "brazil", "australia", "spain", "mexico", "indonesia", "netherlands", "saudiarabia", "turkey", "switzerland","poland",
            "sweden", "belgium", "thailand", "ireland", "argentina", "norway", "israel", "austria", "nigeria", "southafrica",
            "bangladesh", "egypt", "denmark", "singapore", "philippines", "malaysia", "hongkong", "vietnam", "unitedarab", "pakistan",
            "chile","colombia","finland", "romania", "czechia", "newzealand", "portugal", "iran", "peru"]
regions = sorted(regions)

#   connect to database
def Connect():
    conn = pymssql.connect(host='10.51.86.201', user="ZhuJinyan", password="Zhu2022Jinyan", database="Project")
    cur = conn.cursor()
    if not cur:
        raise (NameError, "Connection failed.")
    else:
        return conn, cur


def get_order(hs, time, target, indicator):
    conn, cur = Connect()

    cur.execute('''
            select region, %s from UN_multiple_indicator where hs = %d and time = %d and binomial_cutdown = 0
        ''' % (indicator, hs, time))
    rows = cur.fetchall()
    rows = sorted(rows, key=lambda row: row[1])

    region_order = [row for row in rows if row[0] != target]

    return region_order


def read_csv(path, hs, time, target):
    df = pd.read_csv(os.path.join(path, f'{hs}_results.csv'))

    return df['influence'], df['auc']


def graph(x, y, hs, time, target, indicator):
    plt.scatter(x, y)
    plt.xlabel('score')
    plt.ylabel(indicator)
    plt.title(f'{hs}-{time}-{target}')

    if not os.path.exists('graph'):
        os.mkdir('graph')

    if not os.path.exists(os.path.join('graph', str(hs))):
        os.mkdir(os.path.join('graph', str(hs)))

    plt.savefig(os.path.join('graph', str(hs), f'{time}-{target}-{indicator}.png'))
    plt.close()

def corrcoe(x, y):
    return np.corrcoef(np.array(x), np.array(y))


if __name__ == '__main__':
    indicators = ['degree', 'in_degree', 'out_degree', 'clustering', 'clustering_weight', 'core_number',
                  'degree_centrality',
                  'in_degree_centrality', 'out_degree_centrality', 'eigenvector_centrality',
                  'eigenvector_centrality_wight',
                  'second_order_centrality', 'betweenness_centrality', 'betweenness_centrality_distance',
                  'closeness_centrality',
                  'closeness_centrality_distance', 'degree_weight_Di', 'degree_weight', 'pagerank', 'in_largest_cc',
                  'd_unit', 'd_diff_in',
                  'd_diff_out', 'd_diff_both', 'shs_constraint', 'shs_constraint_weight', 'shs_effective_size',
                  'shs_effective_size_weight',
                  'hits_0', 'hits_1', 'd_an_o_o_None', 'd_an_o_i_None', 'd_an_o_io_None', 'd_an_i_o_None',
                  'd_an_i_i_None',
                  'd_an_i_io_None', 'd_an_io_o_None', 'd_an_io_i_None', 'd_an_io_io_None', 'd_an_o_o_weight',
                  'd_an_o_i_weight',
                  'd_an_o_io_weight', 'd_an_i_o_weight', 'd_an_i_i_weight', 'd_an_i_io_weight', 'd_an_io_o_weight',
                  'd_an_io_i_weight',
                  'd_an_io_io_weight', 'closeness_vitality', 'closeness_vitality_weight',
                  'community_louvain_communities',
                  'community_greedy_modularity_communities', 'community_girvan_newman', 'c_flu_2', 'c_flu_3', 'c_flu_4',
                  'c_flu_5',
                  'c_flu_6', 'c_flu_7', 'c_flu_8', 'c_flu_9', 'community_kernighan_lin_bisection',
                  'community_asyn_lpa_communities',
                  'community_label_propagation_communities']

    for i in range(1, 4):
        folder_path = os.path.join('influ&auc', f'subcount_{i}')

        for hs in range(87, 88):
            if hs == 77 or hs == 98:
                continue

            time = 201001


            y, auc = read_csv(folder_path, hs, time, 'china')

            #graph(x, y, hs, time, target, indicator)
            cc_y = corrcoe(auc, y)

            print(f'{i}:    {cc_y}')

# 201208 only till 201208-nigeria-shs_effective_size_weight

