import sys
import base64
import json
from itertools import groupby
import os


def get_node_dict(file: str) -> dict:
    #  获得{<id>:<pkg-name>}的字典
    with open(file) as f:
        lines = f.readlines()
    lines = list(map(lambda l: l.strip(), lines))
    idList = list(map(lambda l: l.split(',')[0], lines))
    nodeNameList = list(map(lambda l: l.split(',')[1], lines))
    nodeDict = dict(zip(idList, nodeNameList))
    return nodeDict


def get_node_deg(nodeFile: str, nodeDegFile: str) -> dict:
    # 获得{<pkg>:<out-degree>}的字典
    with open(nodeDegFile) as f:
        lines = f.readlines()
    lines = list(map(lambda l: l.strip(), lines))
    nodeDict = get_node_dict(nodeFile)
    nodes = list(map(lambda x: x.split()[0], lines))
    degs = list(map(lambda x: x.split()[1], lines))
    nodeDegDict = dict(zip(list(map(lambda x: nodeDict[x], nodes)), degs))
    return nodeDegDict


def get_edge(file: str) -> list:
    # 获得list(tuple(sourceId, destId))
    with open(file) as f:
        lines = f.readlines()
    edges = list(map(lambda l: tuple((l[0:l.find(' ')], l[l.find(' ') + 1:])), lines))
    return edges


def process_deps(in_file):
    lines = []
    with open(in_file, encoding='utf8') as file:
        lines = file.readlines()
    lines = list(map(lambda l: l.strip(), lines))

    # line格式: <groupid> <artifactid> <version> <base64-encoded json object of deps>
    edgeTuples = []
    for line in lines:
        parts = line.split()
        if len(parts) != 4:
            continue
        key = parts[0] + ':' + parts[1] + ':' + parts[3]  # 忽略version
        rawJson = base64.b64decode(parts[3])
        jsonArray = json.loads(rawJson)
        for recordDict in jsonArray:
            if not recordDict['version']:
                recordDict['version'] = 'None'
            if not recordDict['artifactid']:
                recordDict['artifactid'] = 'None'
            if not recordDict['groupid']:
                recordDict['groupid'] = 'None'

            edgeTuples.append(
                tuple((key.strip(),
                       recordDict['groupid'].strip() + ':' + recordDict['artifactid'].strip() + ':' + recordDict[
                           'version'].strip())))

    # 去掉含参数的maven包, 比如${java.version}
    edgeTuples = list(map(lambda t: tuple((t[0][0:t[0].rfind(':')], t[1][0:t[1].rfind(':')])), edgeTuples))
    edgeTuples = list(filter(lambda t: '{' not in t[0] and '{' not in t[1], edgeTuples))
    edgeTuples = list(filter(lambda t: '...' not in t[0] and '...' not in t[1], edgeTuples))

    nodes = []
    nodeDict = {}
    count = 1

    for key, tuples in groupby(edgeTuples, key=lambda x: x[0]):
        nodes.append(key)

    for key, tuples in groupby(edgeTuples, key=lambda x: x[1]):
        nodes.append(key)

    nodes = sorted(list(set(nodes)))
    for node in nodes:
        nodeDict[node] = str(count)
        count += 1

    # 从信息元组映射到id元组
    edges = list(map(lambda t: tuple((nodeDict[t[0]], nodeDict[t[1]])), edgeTuples))
    # 行去重和排序
    edgeLines = []
    for edge in edges:
        edgeLines.append(edge[0] + ' ' + edge[1] + '\n')
    edgeLines = list(set(edgeLines))
    edgeLines = sorted(edgeLines, key=lambda l: int(l[0:l.find(' ')]) * 100000 + int(l[l.find(' ') + 1:-1]))

    nodeFile = 'mvn-nodes.txt'
    edgeFile = 'mvn-edges.txt'

    if not os.path.exists(nodeFile):
        f = open(nodeFile, 'w')
        f.close()
    if not os.path.exists(edgeFile):
        f = open(nodeFile, 'w')
        f.close()

    with open(nodeFile, 'w', encoding='utf-8') as nf:
        for key in sorted(list(nodeDict.keys())):
            nf.write(nodeDict[key] + ',' + key + '\n')
    with open(edgeFile, 'w', encoding='utf-8') as ef:
        for line in edgeLines:
            ef.write(line)

    # nodeDegDict = dict(zip(nodes, [0 for i in range(len(nodes))]))
    nodeDegDict= {}

    for src, records in groupby(edges, key=lambda e: e[0]):
        nodeDegDict[src] = len(list(records))

    outFile = 'mvn-node-deg.txt'
    if not os.path.exists(outFile):
        f = open(outFile, 'w')
        f.close()

    with open(outFile, 'w', encoding='utf-8') as file:
        for node in nodeDegDict.keys():
            file.write(node + ' ' + str(nodeDegDict[node]) + '\n')
    print('Output mvn-nodes.txt, mvn-edges.txt, mvn-node-deg.txt successfully.')
    print('mvn-nodes.txt: <id>,<pkg>')
    print('mvn-edges.txt: <sourceId> <destId>')
    print('mvn-node-deg.txt: <id> <outDegree>')


def translate_recommendations(recFile: str, nodeFile: str, outFile: str) -> None:
    nodeDict = get_node_dict(nodeFile)
    recLines = []
    with open(recFile) as f:
        recLines = f.readlines()
    recLines = list(map(lambda l: l.strip(), recLines))
    recTuples = []
    for line in recLines:
        if ',' not in line:
            continue
        node = line[0:line.find(':')]
        recs = line[line.find(':') + 1:].split(';')
        recs = list(filter(lambda r: len(r) > 0, recs))
        for rec in recs:
            dest, p = rec.split(',')
            recTuples.append(tuple((nodeDict[node], nodeDict[dest], p)))
    recTuples.sort(key=lambda x: float(x[2]), reverse=True)
    with open(outFile, 'w') as f:
        for t in recTuples:
            f.write(t[0] + ',' + t[1] + ',' + t[2] + '\n')
    print('Output mvn-rec-translated.txt successfully. File format is <source-pkg>,<recommended pkg>,<similarity>')


def warn():
    print('Usage: data_process.py pre-process|translate ...args')
    print(
        'If pre-process, usage is data_process.py pre-process <mvn-dependency-file>. The result is '
        'mvn-nodes.txt, mvn-edges.txt and mvn-node-deg.txt')
    print('If translate, usage is data_process.py pre-process <spark-recommendation-file-path>. mvn-nodes.txt is '
          'needed,The result is mvn-rec-translated.txt')


if __name__ == '__main__':
    args = sys.argv
    if len(args) < 3:
        warn()
    elif args[1] == 'pre-process':
        deps_file = args[2]
        process_deps(deps_file)
    elif args[1] == 'translate':
        translate_recommendations(args[2], 'mvn-nodes.txt', 'mvn-rec-translated.txt')
    else:
        warn()
