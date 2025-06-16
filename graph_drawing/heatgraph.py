import  pymssql
import pandas as pd
import os
import csv
import seaborn as sns
import matplotlib.pyplot as plt


regions = ["usa", "japan", "germany", "unitedkingdom", "india", "france", "italy", "canada", "korea","russia",
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


def read_csv(path, hs, time, target):
    df = pd.read_csv(os.path.join(path, f'{hs}_results.csv'))

    y = []
    for ro in regions:
        data = df[(df['time'] == time) & (df['target'] == target) & (df['attacker'] == ro)]
        y.append([data.iloc[0, 4]])

    return y


def get_indicator(hs, time, indicator, csv_list):
    conn, cur = Connect()

    cur.execute('''
            select region, %s from UN_multiple_indicator where hs = %d and time = %d and binomial_cutdown = 0
        ''' % (indicator, hs, time))
    rows = cur.fetchall()

    for row in rows:
        if row[0] not in regions:
            continue
        csv_list[regions.index(row[0])].append(float(row[1]))

    return csv_list


def figure(data_path):
    data = pd.read_csv(data_path, encoding='utf-8')
    data = data.drop('region', axis=1)
    sns.set(font="simhei")
    plt.figure(figsize=(20, 15))
    plt.rcParams['axes.unicode_minus'] = False
    p1 = sns.heatmap(data.corr(), annot=True)
    plt.show()
    s1 = p1.get_figure()
    s1.savefig('heatmap.jpg')


if __name__ == '__main__':
    indicators = ['betweenness_centrality_distance', 'clustering_weight', 'degree_weight_Di', 'd_unit', 'degree_weight',
                  'closeness_centrality_distance', 'shs_effective_size']

    y_value_path = r'C:\lmf-zzy-project\simulation_attack\influ&auc\subcount_3'
    csv_list = read_csv(y_value_path, 87, 201001, 'china')

    for indicator in indicators:
        csv_list = get_indicator(87, 201001, indicator, csv_list)

    with open('heat.csv', 'w', encoding='utf-8', newline='') as f:
        csvwriter = csv.writer(f)

        csvwriter.writerow(['region', '节点间相互影响力', '加权介数中心性', '加权集聚系数', '有向加权度', '单位权', '无向加权度',
                            '加权紧密中心性', '结构洞有效大小'])
        for i, row in enumerate(csv_list):
            csvwriter.writerow([regions[i]] + row)

    figure('heat.csv')

