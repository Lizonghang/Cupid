import utils
import os

os.system('python segment_partition.py')

MAX_ROUND = 10

if __name__ == '__main__':
    G = utils.create_network_topo_with_old_flows()
    flowinfo = utils.get_flowinfo()
    CL = utils.search_potential_congested_links()
    global_D = utils.create_dependency_graph(utils.get_dependency(CL))
    init_update = utils.update_alone_nodes(G, global_D)

    transition_info = {}
    for CNid in global_D.nodes():
        for nf_dict in utils.map_id_to_CN(CNid):
            nf, fid = utils.dict2tuple(nf_dict)
            if not transition_info.has_key(fid):
                transition_info[fid] = {nf: flowinfo[fid]}
            else:
                transition_info[fid].update({nf: flowinfo[fid]})

    tmp_result = {}
    count = 0
    rest_nf = None

    tmp_result[count] = map(lambda nf_tup: (utils.tuple2dict(nf_tup), flowinfo[nf_tup[1]]), init_update)

    while True:
        count += 1
        print 'Round', count
        tmp_result[count] = []

        die_flow = True

        split_D = utils.split_dependency_graph(global_D)
        for D in split_D:

            US = []

            # Critical node update without deadlocks:
            US_ = []
            step1_flag = False

            CNids_with_no_outdegree = map(lambda i: i[0], filter(lambda i: i[1] == 0, D.out_degree()))

            for CNid in CNids_with_no_outdegree:
                nf_list = utils.map_id_to_CN(CNid)

                for i in xrange(len(nf_list)):
                    nf_dict = nf_list[i]

                    if utils.has_dependency(D, nf_dict):
                        continue

                    nf, fid = utils.dict2tuple(nf_dict)

                    if utils.can_update_in_segment(G, nf, fid, transition_info[fid][nf]):
                        utils.update_segment(G, nf, fid, transition_info[fid][nf])
                        US_.append((nf_dict, transition_info[fid][nf]))
                        utils.remove_nf(global_D, nf_dict)
                        transition_info[fid][nf] = 0.0

            if US_:
                US += US_
                step1_flag = True
                die_flow = False

            # Schedulable critical node update in deadlock
            US_ = []
            step2_flag = False
            if not step1_flag:
                nf_list = utils.get_all_nf(D)

                record = []
                for nf_dict in nf_list:
                    nf, fid = utils.dict2tuple(nf_dict)
                    if utils.can_update_in_segment(G, nf, fid, transition_info[fid][nf]):
                        record.append((nf_dict, transition_info[fid][nf]))

                record.sort(key=lambda x: x[1], reverse=True)

                for nf_dict, size in record:
                    nf, fid = utils.dict2tuple(nf_dict)
                    if utils.can_update_in_segment(G, nf, fid, size):
                        utils.update_segment(G, nf, fid, size)
                        US_.append((nf_dict, size))
                        utils.remove_nf(global_D, nf_dict)
                        transition_info[fid][nf] = 0.0

            if US_:
                US += US_
                step2_flag = True
                die_flow = False

            # Multipath Transition
            US_ = []
            if not step1_flag and not step2_flag:
                locks = utils.find_deadlock(D)
                option = []
                for lock in locks:
                    for nf, fid in lock:
                        Pn = utils.get_path_to_next_critical_node(nf, fid, 'new')
                        Po = utils.get_path_to_next_critical_node(nf, fid, 'old')
                        En = utils.get_edges_on_path(Pn, with_weights=False)
                        Eo = utils.get_edges_on_path(Po, with_weights=False)

                        avail_bw = []
                        for e in En:
                            params = G[e[0]][e[1]]
                            avail_bw.append(round(params['weight'] - params['bw'], 2))
                        min_avail_bw = min(avail_bw)

                        ab = min(transition_info[fid][nf], min_avail_bw)

                        if ab > 0.0:
                            option.append(((nf, fid), ab))

                if option:
                    option.sort(key=lambda tup: tup[1], reverse=True)
                    update_nf_tup, ab = option[0]
                    nf, fid = update_nf_tup

                    utils.update_segment(G, nf, fid, ab)

                    US_.append((utils.tuple2dict(update_nf_tup), ab))
                    transition_info[fid][nf] = round(transition_info[fid][nf] - ab, 2)
                    if transition_info[fid][nf] == 0.0:
                        utils.remove_nf(global_D, utils.tuple2dict(update_nf_tup))

            if US_:
                US += US_
                die_flow = False

            if US:
                print US
                tmp_result[count] += US

        if not global_D.nodes():
            break

        if count >= MAX_ROUND or die_flow:
            rest_nf = utils.get_all_nf(global_D)
            print
            print 'force move:', rest_nf
            tmp_result[count+1] = []

            overload_map = {}
            overload_G = G.copy()

            for nf_dict in rest_nf:
                nf, fid = utils.dict2tuple(nf_dict)
                utils.update_segment(G, nf, fid, transition_info[fid][nf])
                utils.remove_nf(global_D, nf_dict)
                utils.update_segment_without_moving_out(overload_G, nf, fid, transition_info[fid][nf])
                tmp_result[count+1].append((nf_dict, transition_info[fid][nf]))
                transition_info[fid][nf] = 0.0

            for e in overload_G.edges():
                params = overload_G[e[0]][e[1]]
                if params['bw'] > params['weight']:
                    overload_map[e] = round(round(params['bw'] - params['weight'], 2) / params['weight'], 2)

            if overload_map:
                print
                print 'overload map:'
                print overload_map

            break

    result = {}
    for r in tmp_result:
        result[r] = []
        for item in tmp_result[r]:
            result[r].append(item)

    print
    print 'Round', '\t', 'State', '\t', 'Num', '\t', 'FlowSize'
    for r in result:
        print r, '\t',
        if r == count + 1:
            print 'force', '\t',
        else:
            print '\t',
        print len(result[r]), '\t', sum([item[1] for item in result[r]])

    print
    print 'update order:'
    print result

    complete_round_map = {}
    for fid in flowinfo:
        complete_round_map[fid] = 0

    for r in result:
        for nf_tup in result[r]:
            nf, fid = utils.dict2tuple(nf_tup[0])
            complete_round_map[fid] = r

    utils.save_complete_round(complete_round_map)

    print
    print 'complete round:'
    print complete_round_map
