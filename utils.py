import networkx as nx


def add_nodes_from_file(G_, path='topo/nodes.txt'):
    with open(path) as fp:
        nodes_list = map(lambda node: node.strip(), fp.readlines())
        G_.add_nodes_from(nodes_list)


def add_edges_from_file(G_, path='topo/edges.txt'):
    edge_list = []
    with open(path) as fp:
        line = fp.readline().strip()
        while line:
            items = line.split()
            e = (items[0], items[1], float(items[2]))
            edge_list.append(e)
            line = fp.readline().strip()
    G_.add_weighted_edges_from(edge_list)


def get_flowinfo(path='topo/flowinfo.txt'):
    flowinfo = {}
    with open(path) as fp:
        line = fp.readline().strip()
        while line:
            items = line.split()
            fid = int(items[0])
            bw = float(items[1])
            flowinfo[fid] = bw
            line = fp.readline().strip()
    return flowinfo


def get_flow(fid, version):
    assert version in ['new', 'old']
    with open('topo/flow_%s.txt' % version) as fp:
        line = fp.readline().strip()
        while line:
            items = line.split(',')
            if fid == int(items[0]):
                return items[1].strip().split()
            line = fp.readline().strip()


def get_edges_on_path(P, with_weights=False):
    edges = []
    for i in xrange(len(P)-1):
        edges.append((P[i], P[i+1]))

    if not with_weights:
        return edges

    edges_with_weights = []
    with open('topo/edges.txt') as fp:
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


def get_critical_nodes_on_flow(fid):
    Pn = get_flow(fid, 'new')
    G_ = create_flow_graph(fid, split=False, with_weights=False)

    # return nodes which in-degree or out-degree is 2
    critical_nodes = []
    for node in Pn:
        if G_.in_degree(node) == 2 or G_.out_degree(node) == 2:
            critical_nodes.append(node)

    return critical_nodes


def get_edges_in_circle(cycle):
    edges = []
    for i in xrange(len(cycle)):
        n = cycle[i % len(cycle)]
        m = cycle[(i + 1) % len(cycle)]
        edges.append((n, m))
    return edges


def in_circle(cycles, e):
    for cycle in cycles:
        edges_on_cycle = get_edges_in_circle(cycle)
        if e in edges_on_cycle:
            return True
    return False


def get_successor(G_, n):
    if not n:
        return None

    try:
        m = list(G_.successors(n))[0]
        return m
    except IndexError:
        return None


def get_predecessor(G_, n):
    if not n:
        return None

    try:
        m = list(G_.predecessors(n))[0]
        return m
    except IndexError:
        return None


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


def find_nf(fid, version, l):
    Gn, Go = create_flow_graph(fid, split=True, with_weights=False)
    critical_nodes = get_critical_nodes_on_flow(fid)

    G_ = Gn if version == 'new' else Go

    nf = l[0]
    while nf and nf not in critical_nodes:
        nf = get_predecessor(G_, nf)

    assert nf is not None
    return nf


def get_dependency(CL):
    D_ = []

    for l in CL:
        Fn = get_flows_through_l(l, 'new')
        Fo = get_flows_through_l(l, 'old')

        CN_Fn_l = []
        CN_Fo_l = []
        for fid in Fn:
            CN_Fn_l.append({find_nf(fid, 'new', l): fid})
        for fid in Fo:
            CN_Fo_l.append({find_nf(fid, 'old', l): fid})

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
        edge_labels[e] = G[e[0]][e[1]]['bw']
    nx.draw(G, pos=nx.spectral_layout(G), with_labels=True)
    nx.draw_networkx_edge_labels(G, pos=nx.spectral_layout(G), font_size=5, edge_labels=edge_labels)
    plt.show()


def draw_graph(G):
    import matplotlib.pyplot as plt
    nx.draw(G, pos=nx.spring_layout(G), with_labels=True)
    plt.show()


def get_segmentid(fid, path='topo/flow_segmentid_map.txt'):
    with open(path) as fp:
        line = fp.readline().strip()
        while line:
            items = line.split()
            if fid == int(items[0]):
                flow_segmentid = map(lambda item: int(item), items[1:])
                return flow_segmentid
            line = fp.readline().strip()


def map_segmentid_to_segments(segmentid, path='topo/segmentid_segment_map.txt'):
    with open(path) as fp:
        line = fp.readline().strip()
        while line:
            items = line.split()
            if segmentid == int(items[0]):
                return (items[1], items[2])
            line = fp.readline().strip()


def get_nodes_in_dependency_graph(D):
    nodes = []
    for CN_id in D.nodes():
        nodes += map(lambda nf: (nf.keys()[0], nf.values()[0]), map_id_to_CN(CN_id))
    return set(nodes)


def get_path_to_next_critical_node(nf, fid, version):
    critical_nodes = get_critical_nodes_on_flow(fid)
    P = get_flow(fid, version)

    path = [nf]

    try:
        i = P.index(nf) + 1
    except ValueError:
        return None

    if i == len(P):
        return None

    while P[i] not in critical_nodes and i < len(P) - 1:
        path.append(P[i])
        i += 1

    path.append(P[i])

    if i == len(P):
        return None
    else:
        return path


def update_segment(G, nf, fid, size):
    Pn = get_path_to_next_critical_node(nf, fid, 'new')
    Po = get_path_to_next_critical_node(nf, fid, 'old')

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


def update_alone_nodes(G, D):
    nodes_in_dependency_graph = get_nodes_in_dependency_graph(D)
    flowinfo = get_flowinfo()
    for fid in flowinfo:
        nodes = set(get_flow(fid, 'new') + get_flow(fid, 'old'))
        nodes = map(lambda nf: (nf, fid), nodes)
        nodes = filter(lambda nf: nf not in nodes_in_dependency_graph, nodes)
        for node in nodes:
            update_segment(G, node[0], node[1], flowinfo[fid])


def can_update_in_segment(G, nf, fid, size):
    G = G.copy()
    update_segment(G, nf, fid, size)
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


def find_deadlock(D):
    tmp_lock = []

    for nf_dict in get_all_nf(D):
        root = dict2tuple(nf_dict)
        T_nf = nx.DiGraph()
        T_nf.add_node(root)

        queue = [root]
        record = [root]
        while queue:
            nf_tup = queue[0]
            d_nodes = map(dict2tuple, find_dependency(D, tuple2dict(nf_tup)))
            d_edges = map(lambda node: (nf_tup, node), d_nodes)
            T_nf.add_edges_from(d_edges)

            for d_node in d_nodes:
                if d_node not in record:
                    queue.append(d_node)
                    record.append(d_node)
            queue.remove(nf_tup)

        tmp_lock.append(list(nx.simple_cycles(T_nf)))

    deadlock = []
    for locks_of_nf in tmp_lock:
        if not deadlock:
            deadlock.append(locks_of_nf)
            continue
        # find intersection
        index = -1
        for locks_of_group in deadlock:
            for lock in locks_of_nf:
                if lock in locks_of_group:
                    index = deadlock.index(locks_of_group)
                    break
            if index != -1:
                break
        # union or new
        if index != -1:
            for lock in locks_of_nf:
                if lock not in deadlock[index]:
                    deadlock[index].append(lock)
        else:
            deadlock.append(locks_of_nf)

    return deadlock


def is_nf_in_locks(nf, locks):
    nf_tup = dict2tuple(nf)
    for lock in locks:
        if nf_tup in lock:
            return True
    return False
