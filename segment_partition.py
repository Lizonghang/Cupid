# coding=utf-8
import utils
import networkx as nx


def segment_partition(fid, critical_nodes):
    Pn = utils.get_flow(fid, 'new')
    Gn, Go = utils.create_flow_graph(fid, split=True, with_weights=False)
    G_ = utils.create_flow_graph(fid, split=False, with_weights=False)
    cycles = list(nx.simple_cycles(G_))

    # Algorithm 1: Segment Partition
    segments = []
    n = Pn[0]
    while n in Pn:
        s = [n]
        m = utils.get_successor(Gn, n)
        while m and (m not in critical_nodes or utils.in_circle(cycles, (n, m))):
            s.append(m)
            n = m
            m = utils.get_successor(Gn, n)
        if m:
            s.append(m)
        segments.append(s)
        n = m

    return segments


if __name__ == '__main__':
    fp1 = open('topo/flow_segmentid_map.txt', 'w')
    fp2 = open('topo/segmentid_segment_map.txt', 'w')
    fp3 = open('topo/segmentid_segment_map_full.txt', 'w')

    flowinfo_map = utils.get_flowinfo()
    segment_id = 1
    for fid in flowinfo_map:
        critical_nodes = utils.get_critical_nodes_on_flow(fid)
        segments = segment_partition(fid, critical_nodes)

        # map path in segments to critical node-pairs
        segments = filter(lambda segment: len(segment) > 1, segments)
        raw_segments = segments[:]
        for i in xrange(len(segments)):
            segments[i] = [segments[i][0], segments[i][-1]]

        segment_id_list = []
        for i in xrange(len(segments)):
            s = segments[i]
            raw_s = raw_segments[i]
            fp2.write(' '.join([str(segment_id)] + s) + '\n')
            fp3.write(' '.join([str(segment_id)] + raw_s) + '\n')
            segment_id_list.append(segment_id)
            segment_id += 1
        fp1.write(' '.join([str(fid), ' '.join(map(lambda id: str(id), segment_id_list))]) + '\n')
        print 'partitions of flow %s: ' % fid + ' '.join(map(lambda path: '->'.join(path), raw_segments))

    fp1.close()
    fp2.close()
    fp3.close()

    print
