import utils
import os
import random

os.system('python segment_partition.py')

if __name__ == '__main__':
    G = utils.create_network_topo_with_old_flows()
    flowinfo = utils.get_flowinfo()
    CL = utils.search_potential_congested_links()
    D = utils.create_dependency_graph(utils.get_dependency(CL))
    utils.update_alone_nodes(G, D)

    transition_info = {}
    for CNid in D.nodes():
        for nf_dict in utils.map_id_to_CN(CNid):
            nf, fid = utils.dict2tuple(nf_dict)
            if not transition_info.has_key(fid):
                transition_info[fid] = {nf: flowinfo[fid]}
            else:
                transition_info[fid].update({nf: flowinfo[fid]})

    while True:
        US = []
        # Critical node update without deadlocks:
        while True:
            CNids_with_no_outdegree = map(lambda i: i[0], filter(lambda i: i[1] == 0, D.out_degree()))

            if not CNids_with_no_outdegree:
                break

            US_ = []
            for CNid in CNids_with_no_outdegree:
                nf_list = utils.map_id_to_CN(CNid)

                for i in xrange(len(nf_list)):
                    nf_dict = nf_list[i]

                    if utils.has_dependency(D, nf_dict):
                        continue

                    nf, fid = utils.dict2tuple(nf_dict)

                    if utils.can_update_in_segment(G, nf, fid, transition_info[fid][nf]):
                        utils.update_segment(G, nf, fid, transition_info[fid][nf])
                        US_.append(nf_dict)
                        utils.remove_nf(D, nf_dict)

            if US_:
                US.append(US_)
            else:
                break

        print 'Step 1, US:', US

        US = []
        # Schedulable critical node update in deadlock
        while True:
            if not D.nodes():
                break

            US_ = []
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
                    US_.append(nf_dict)
                    utils.remove_nf(D, nf_dict)

            if US_:
                US.append(US_)
            else:
                break

        print 'Step 2, US:', US

        US = []
        # Multipath Transition
        handle_locks = []
        for group in utils.find_deadlock(D):
            handle_locks.append(random.choice(group))

        for locks in handle_locks:
            US_ = []

            option = []
            for nf, fid in locks:
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

                US_.append(utils.tuple2dict(update_nf_tup))
                US += US_

                transition_info[fid][nf] = round(transition_info[fid][nf] - ab, 2)
                if transition_info[fid][nf] == 0.0:
                    locks.remove(update_nf_tup)
                    utils.remove_nf(D, utils.tuple2dict(update_nf_tup))

        print 'Step 3, US:', [US]

        if not D.nodes():
            break
