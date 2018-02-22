
import networkx as nx
import matplotlib.pyplot as plt

import json
import logging

DRAW_GRAPHS = False
GENERATE_COOCCURENCE_GRAPHS_NX = False
LOG_LEVEL = logging.DEBUG

logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)


####
## Creating a Graph Using NetworkX
####

def generate_noun_graph(corpus_links, corpus):
    # For networkx tutorial:
    # https://networkx.github.io/documentation/stable/tutorial.html

    # Instantiate Graph
    noun_graph = nx.Graph()

    # Create Nodes
    num_nodes = len(corpus)
    noun_graph.add_nodes_from(corpus)

    # Create Links
    edge_list = []
    for ix_x in range(num_nodes):
        logging.info('NOUN GRAPH: Working on node {}/{}: {}'.format(ix_x, num_nodes, corpus[ix_x]))
        for ix_y in range(ix_x):
            if ix_x > 3667:
                logging.info('PROBLEM NODE {}/{}: {}'.format(ix_y, num_nodes, corpus[ix_y]))
            node_x = corpus[ix_x]
            node_y = corpus[ix_y]
            edge_list.append((node_x, node_y, {'weight':corpus_links[ix_x, ix_y]}))
    noun_graph.add_edges_from(edge_list)

    # Draw graph
    if DRAW_GRAPHS:
        nx.draw_networkx(noun_graph, pos=nx.spring_layout(noun_graph))
        plt.draw()

    return noun_graph

def generate_minimum_spanning_tree_plus(noun_graph, corpus_links, cutoff, corpus):
    # Generate minimum spanning tree
    mst = nx.minimum_spanning_tree(noun_graph)
    num_nouns = len(corpus)
    # Enforce that all links with weight above the cutoff are included
    edge_list = []
    for ix_x in range(corpus_links.shape[0]):
        logging.info('MINIMUM SPANNING TREE: Working on noun {}/{}: {}'.format(ix_x, num_nouns, corpus[ix_x]))
        for ix_y in range(ix_x):
            link_weight = corpus_links[ix_x, ix_y]
            if link_weight > cutoff:
                node_x = corpus[ix_x]
                node_y = corpus[ix_y]
                edge_list.append((node_x, node_y, {'weight':link_weight}))
    mst.add_edges_from(edge_list)

    # Draw graph
    if DRAW_GRAPHS:
        nx.draw_networkx(mst, pos=nx.spring_layout(mst))
        plt.draw()

    return mst


####
## Workflow
####


if GENERATE_COOCCURENCE_GRAPHS_NX:

    ####
    ## Create cooccurence graphs
    ####
    corpus_links = pickle.load(open('data/corpus_links.pkl', 'rb'))

    with open('data/entities.txt', 'r') as f:
        corpus = json.loads(f.read())

    # Full noun_graph
    noun_graph = generate_noun_graph(corpus_links, corpus)
    nx.write_gpickle(mstp, 'full_noun_graph.pkl')

    # Minimal spanning tree with additional links based on cutoff
    cutoff = .5
    mstp = generate_minimum_spanning_tree_plus(noun_graph, corpus_links, cutoff, corpus)
    nx.write_gpickle(mstp, 'minimal_spanning_tree_cutoff_{}.pkl'.format(cutoff))
