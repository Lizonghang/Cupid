import networkx as nx


def add_nodes_from_file(G_, path='topo/nodes.txt'):
    with open(path) as fp:
        nodes_list = map(lambda node: node.strip(), fp.readlines())
        G_.add_nodes_from(nodes_list)


def add_edges_from_file(G_, path='topo/link_capacity.txt'):
    edge_list = []
    with open(path) as fp:
        line = fp.readline().strip()
        while line:
            items = line.split()
            e = (items[0], items[1], float(items[2]))
            edge_list.append(e)
            line = fp.readline().strip()
    G_.add_weighted_edges_from(edge_list)


def get_flowinfo(path='topo/flow_demand.txt'):
    flowinfo = {}
    with open(path) as fp:
        line = fp.readline().strip()
        while line:
            items = map(lambda _: _.strip(), line.split(','))
            fid = int(items[0])
            bw = float(items[1])
            flowinfo[fid] = round(bw * 0.6, 2)
            line = fp.readline().strip()
    return flowinfo


def get_flow(fid, version):
    assert version in ['new', 'old']
    with open('topo/%sflow.txt' % version) as fp:
        line = fp.readline().strip()
        while line:
            items = map(lambda _: _.strip(), line.split(','))
            if fid == int(items[0]):
                return items[1].split()
            line = fp.readline().strip()


def get_edges_on_path(P, with_weights=False):
    edges = []
    for i in xrange(len(P)-1):
        edges.append((P[i], P[i+1]))

    if not with_weights:
        return edges

    edges_with_weights = []
    with open('topo/link_capacity.txt') as fp:
        for e in edges:
            fp.seek(0)
            flag = False

            line = fp.readline().strip()
            while line:
                items = line.split()
                if e[0] == items[0] and e[1] == items[1]:
                    edges_with_weights.append((items[0], items[1], float(items[2])))
                    flag = True
                    break
                line = fp.readline().strip()

            if not flag:
                raise ValueError("edge {}->{} is not exist on network topo.".format(e[0], e[1]))

    return edges_with_weights


def create_flow_graph(fid, split=False, with_weights=False):
    Pn = get_flow(fid, 'new')
    Po = get_flow(fid, 'old')
    if split:
        temp_Gn = nx.DiGraph()
        temp_Go = nx.DiGraph()
        if with_weights:
            temp_Gn.add_weighted_edges_from(get_edges_on_path(Pn, with_weights=True))
            temp_Go.add_weighted_edges_from(get_edges_on_path(Po, with_weights=True))
        else:
            temp_Gn.add_edges_from(get_edges_on_path(Pn, with_weights=False))
            temp_Go.add_edges_from(get_edges_on_path(Po, with_weights=False))
        return temp_Gn, temp_Go
    else:
        temp_G = nx.DiGraph()
        if with_weights:
            temp_G.add_weighted_edges_from(get_edges_on_path(Pn, with_weights=True))
            temp_G.add_weighted_edges_from(get_edges_on_path(Po, with_weights=True))
        else:
            temp_G.add_edges_from(get_edges_on_path(Pn, with_weights=False))
            temp_G.add_edges_from(get_edges_on_path(Po, with_weights=False))
        return temp_G


def get_edges_in_circle(cycle):
    edges = []
    for i in xrange(len(cycle)):
        n = cycle[i % len(cycle)]
        m = cycle[(i + 1) % len(cycle)]
        edges.append((n, m))
    return edges


def create_network_topo():
    # get all flows
    flow_map = {}
    flowid_list = get_flowinfo()
    for fid in flowid_list:
        flow_map[fid] = {}
        flow_map[fid]['new'] = get_edges_on_path(get_flow(fid, 'new'), with_weights=True)
        flow_map[fid]['old'] = get_edges_on_path(get_flow(fid, 'old'), with_weights=True)

    # construct network topo with weights (capacity)
    G_ = nx.DiGraph()
    add_nodes_from_file(G_)
    add_edges_from_file(G_)
    return G_


def init_network_bandwidth(G_):
    for e in G_.edges():
        G_[e[0]][e[1]]['bw'] = 0.0
    return G_


def inject_old_flows(G):
    flowinfo = get_flowinfo()
    for fid in flowinfo:
        Po = get_flow(fid, 'old')
        Eo = get_edges_on_path(Po, with_weights=False)
        for e in Eo:
            G[e[0]][e[1]]['bw'] += flowinfo[fid]
    return G


def search_potential_congested_links():
    G_ = create_network_topo()
    G_ = init_network_bandwidth(G_)

    flowinfo = get_flowinfo()
    for fid in flowinfo:
        Pn = get_flow(fid, 'new')
        Po = get_flow(fid, 'old')
        En = get_edges_on_path(Pn, with_weights=False)
        Eo = get_edges_on_path(Po, with_weights=False)
        Ef = set(En + Eo)
        bw = flowinfo[fid]

        for e in Ef:
            G_[e[0]][e[1]]['bw'] += bw

    CL = []
    for e in G_.edges():
        e_info = G_[e[0]][e[1]]
        if e_info['bw'] > e_info['weight']:
            CL.append(e)

    return CL


def get_flows_through_l(l, version):
    Fl = []
    flowinfo = get_flowinfo()
    for fid in flowinfo:
        P = get_flow(fid, version)
        E = get_edges_on_path(P)
        if l in E:
            Fl.append(fid)
    return Fl


def get_dependency(CL):
    D_ = []

    for l in CL:
        Fn = get_flows_through_l(l, 'new')
        Fo = get_flows_through_l(l, 'old')

        CN_Fn_l = []
        CN_Fo_l = []
        for fid in Fn:
            s = get_flow(fid, 'new')[0]
            CN_Fn_l.append({s: fid})
        for fid in Fo:
            s = get_flow(fid, 'old')[0]
            CN_Fo_l.append({s: fid})
        if CN_Fn_l and CN_Fo_l:
            D_.append((CN_Fn_l, CN_Fo_l))

    D = []
    for d in D_:
        if d not in D:
            D.append(d)

    return D


def save_and_map_to_id(D_, path='topo/dependency_map.txt'):
    dependency_graph = []
    nodeid = 1
    with open(path, 'w') as fp:
        for d in D_:
            fp.write(str(nodeid) + ' ' +
                     ' '.join(map(lambda item: item.keys()[0] + " " + str(item.values()[0]), d[0])) + '\n')
            fp.write(str(nodeid + 1) + ' ' +
                     ' '.join(map(lambda item: item.keys()[0] + " " + str(item.values()[0]), d[1])) + '\n')
            dependency_graph.append((nodeid, nodeid + 1))
            nodeid += 2
    return dependency_graph


def map_id_to_CN(nodeid, path='topo/dependency_map.txt'):
    CN = []
    with open(path) as fp:
        line = fp.readline().strip()
        while line:
            items = line.split()
            if int(items[0]) == nodeid:
                for i in xrange((len(items) - 1) / 2):
                    nf = {items[1+2*i]: int(items[2+2*i])}
                    CN.append(nf)
                return CN
            line = fp.readline().strip()
    return []


def create_dependency_graph(D_):
    D_ = save_and_map_to_id(D_)
    D = nx.DiGraph()
    D.add_edges_from(D_)
    return D


def create_network_topo_with_old_flows():
    G = create_network_topo()
    G = init_network_bandwidth(G)
    G = inject_old_flows(G)
    return G


def draw_graph_with_bw(G):
    import matplotlib.pyplot as plt
    edge_labels = {}
    for e in G.edges():
        edge_labels[e] = str(G[e[0]][e[1]]['bw']) + '/' + str(G[e[0]][e[1]]['weight'])
    nx.draw(G, pos=nx.spectral_layout(G), with_labels=True)
    nx.draw_networkx_edge_labels(G, pos=nx.spectral_layout(G), font_size=5, edge_labels=edge_labels)
    plt.show()


def draw_graph(G, layout=nx.spring_layout):
    import matplotlib.pyplot as plt
    nx.draw(G, pos=layout(G), with_labels=True)
    plt.show()


def get_nodes_in_dependency_graph(D):
    nodes = []
    for CN_id in D.nodes():
        nodes += map(lambda nf: (nf.keys()[0], nf.values()[0]), map_id_to_CN(CN_id))
    return set(nodes)


def update_segment(G, fid, size):
    Pn = get_flow(fid, 'new')
    Po = get_flow(fid, 'old')

    if not Pn or not Po:
        return

    En = get_edges_on_path(Pn, with_weights=False)
    Eo = get_edges_on_path(Po, with_weights=False)

    for e in Eo:
        params = G[e[0]][e[1]]
        params['bw'] = round(params['bw'] - size, 2)

    for e in En:
        params = G[e[0]][e[1]]
        params['bw'] += size


def recover_flow(G, fid, size):
    Pn = get_flow(fid, 'new')
    En = get_edges_on_path(Pn, with_weights=False)
    for e in En:
        params = G[e[0]][e[1]]
        params['bw'] += size


def remove_flow(G, fid, size):
    Po = get_flow(fid, 'old')
    Eo = get_edges_on_path(Po, with_weights=False)
    for e in Eo:
        params = G[e[0]][e[1]]
        params['bw'] = round(params['bw'] - size, 2)


def update_alone_nodes(G, D):
    nodes_in_dependency_graph = get_nodes_in_dependency_graph(D)
    flowinfo = get_flowinfo()
    init_update = []
    for fid in flowinfo:
        s = get_flow(fid, 'new')[0]
        if (s, fid) not in nodes_in_dependency_graph:
            update_segment(G, fid, flowinfo[fid])
            init_update.append((s, fid))
    return init_update


def can_update_in_segment(G, fid, size):
    G = G.copy()
    update_segment(G, fid, size)
    for e in G.edges():
        params = G[e[0]][e[1]]
        if params['bw'] > params['weight']:
            return False
    return True


def load_dependency_map(path='topo/dependency_map.txt'):
    with open(path) as fp:
        lines = filter(lambda line: line.strip(), fp.readlines())
        return map(lambda line: line.strip(), lines)


def remove_nf(D, nf_, path='topo/dependency_map.txt'):
    map_file = load_dependency_map()
    for CNid in list(D.nodes()):
        nodes_in_CN = map_id_to_CN(CNid)
        for nf in nodes_in_CN:
            if nf == nf_:
                nodes_in_CN.remove(nf_)
                for i in xrange(len(map_file)):
                    if map_file[i] == '':
                        continue
                    items = map_file[i].split()
                    if CNid == int(items[0]):
                        if not nodes_in_CN:
                            map_file[i] = ''
                            D.remove_node(CNid)
                        else:
                            map_file[i] = str(CNid) + ' ' + ' '.join(
                                map(lambda nf: ' '.join([nf.keys()[0], str(nf.values()[0])]), nodes_in_CN)
                            )

    map_file = filter(lambda line: line, map_file)
    map_file = map(lambda line: line + '\n', map_file)
    with open(path, 'w') as fp:
        fp.writelines(map_file)


def has_dependency(D, nf):
    for CNid in map(lambda i: i[0], filter(lambda i: i[1] != 0, D.out_degree())):
        if nf in map_id_to_CN(CNid):
            return True
    return False


def get_all_nf(D):
    nf_list = []
    for CNid in D.nodes():
        for nf in map_id_to_CN(CNid):
            if nf not in nf_list:
                nf_list.append(nf)
    return nf_list


def find_dependency(D, nf_):
    dependency_list = []
    for CNid_Fn, CNid_Fo in D.edges():
        if nf_ in map_id_to_CN(CNid_Fn):
            for nf in map_id_to_CN(CNid_Fo):
                dependency_list.append(nf)
    return dependency_list


def dict2tuple(nf_dict):
    return nf_dict.keys()[0], int(nf_dict.values()[0])


def tuple2dict(nf_tup):
    return {nf_tup[0]: nf_tup[1]}


def has_intersection(arr1, arr2):
    for nf_tup in arr1:
        if nf_tup in arr2:
            return True
    return False


def find_connected_subgraphs(D):
    all_nf = get_all_nf(D)
    dependency_map = {}
    for nf_dict in all_nf:
        dependency_map[dict2tuple(nf_dict)] = find_dependency(D, nf_dict)

    visible_map = {}
    for nf_dict in all_nf:
        root = dict2tuple(nf_dict)

        queue = [root]
        record = [root]
        while queue:
            nf_tup = queue[0]
            d_nodes = map(dict2tuple, dependency_map[nf_tup])
            for d_node in d_nodes:
                if d_node not in record:
                    queue.append(d_node)
                    record.append(d_node)
            queue.pop(0)

        visible_map[root] = record

    roots = visible_map.keys()

    # check if can merge
    i, j = 0, 1
    while i < len(roots) and j < len(roots):
        if has_intersection(visible_map[roots[i]], visible_map[roots[j]]):
            for nf_tup in visible_map[roots[j]]:
                if nf_tup not in visible_map[roots[i]]:
                    visible_map[roots[i]].append(nf_tup)
            visible_map.pop(roots[j])
            roots.remove(roots[j])
        else:
            j += 1

        if j == len(roots):
            i += 1
            j = i + 1

    return visible_map.values()


def split_dependency_graph(D):
    subgraph = find_connected_subgraphs(D)

    subD_arr = []
    for i in xrange(len(subgraph)):
        subD_arr.append([])

    for CNid in D.nodes():
        nf_tup = dict2tuple(map_id_to_CN(CNid)[0])
        for subg in subgraph:
            if nf_tup in subg:
                subD_arr[subgraph.index(subg)].append(CNid)
                break

    split_D = []

    for CNids in subD_arr:
        subD = nx.DiGraph()
        for CNid in CNids:
            if not subD.has_node(CNid):
                subD.add_node(CNid)
            if D[CNid]:
                next_CNid = D[CNid].keys()[0]
                subD.add_edge(CNid, next_CNid)
        split_D.append(subD)

    return split_D


def save_complete_round(complete_round, path='topo/round.txt'):
    with open(path, 'w') as fp:
        for fid in complete_round:
            fp.write(str(fid) + ',' + str(complete_round[fid]) + '\n')
