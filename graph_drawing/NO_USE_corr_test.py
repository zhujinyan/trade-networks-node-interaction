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


def read_csv(path, hs, time, target, region_order):
    df = pd.read_csv(os.path.join(path, f'{hs}_results.csv'))

    y = []
    for ro in region_order:
        data = df[(df['time'] == time) & (df['target'] == target) & (df['attacker'] == ro[0])]
        y.append(data.iloc[0, 4])

    return y


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
    indicators = ['pagerank']

    hs = 87
    time = 201306

    for indicator in indicators:
        target = 'italy'
        order = get_order(hs, time, target, indicator)

        x = [row[1] for row in order]
        y = read_csv('', hs, time, target, order)

        #graph(x, y, hs, time, target, indicator)
        cc = corrcoe(x, y)
        print(cc)

# 201208 only till 201208-nigeria-shs_effective_size_weight

