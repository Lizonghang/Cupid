def reformat(fn):
    with open(fn, 'r') as fp:
        lines = map(lambda _: _.strip(), fp.readlines())
        for i in xrange(len(lines)):
            lines[i] = str(i+1) + ',' + lines[i] + '\n'

    with open(fn, 'w') as fp:
        fp.writelines(lines)


def handle_node():
    with open('topo/link_capacity.txt', 'r') as fp:
        lines = map(lambda _: _.strip(), fp.readlines())
        for i in xrange(len(lines)):
            items = lines[i].split()
            lines[i] = str(int(items[0])-1) + ' ' + str(int(items[1])-1) + ' ' + items[2] + '\n'

    with open('topo/link_capacity.txt', 'w') as fp:
        fp.writelines(lines)

    with open('topo/nodes.txt', 'r') as fp:
        lines = map(lambda _: _.strip(), fp.readlines())
        for i in xrange(len(lines)):
            lines[i] = str(int(lines[i])-1) + '\n'

    with open('topo/nodes.txt', 'w') as fp:
        fp.writelines(lines)


if __name__ == '__main__':
    reformat('topo/flow_demand.txt')
    reformat('topo/newflow.txt')
    reformat('topo/oldflow.txt')
    handle_node()
