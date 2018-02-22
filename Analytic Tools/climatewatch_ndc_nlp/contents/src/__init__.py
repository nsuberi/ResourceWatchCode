####
## Import required libraries
####

from bs4 import BeautifulSoup

from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types

import numpy as np
import networkx as nx

import subprocess
import glob
import os
import logging
import sys
import json
import pickle

LOG_LEVEL = logging.DEBUG
logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)

SALIENCE_THRESHOLD = .001
DRAW_GRAPHS = False
PROCESS_DATA = False
CREATE_LINKS = False
GENERATE_COOCCURENCE_GRAPHS = True

####
## Preparing Data
####

def convert_full_html_to_text(original_docs):
    documents = {}
    for doc in original_docs:
        country_name = doc.split('/')[1].split('-')[0]
        with open(doc, 'r') as f:
            soup = BeautifulSoup(f.read(), 'lxml')
            all_text = ''.join(soup.findAll(text=True))
            documents[country_name] = all_text
    return documents

# https://stackoverflow.com/questions/40716272/how-to-extract-h1-tag-text-with-beautifulsoup
def extract_html_section_to_text(original_docs, section):
    documents = {}
    for doc in original_docs:
        country_name = doc.split('/')[1].split('-')[0]
        with open(doc, 'r') as f:
            soup = BeautifulSoup(f.read(), 'lxml')
            sections = soup.select(section)
            section_text = ''.join(sections.findAll(text=True))
            documents[country_name] = section_text
    return documents

def break_html_by_sections(original_docs, breakpt):
    # TO DO: break document by section,
    # so that you return all text in between two h# tags
    # i.e. Everything between the h3 tags that represent numbered sections in english_ndcs
    pass
    return original_docs

####
## Machine Learning for Natural Language Processing
####

with open('gcsPrivateKey.json', 'w') as f:
    f.write(os.environ['GEE_JSON'])
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'gcsPrivateKey.json'

# Instantiate a client
client = language.LanguageServiceClient()

# Structure of response: https://cloud.google.com/natural-language/docs/reference/rpc/google.cloud.language.v1#analyzeentitiesresponse
# Entities is a protocol buffer, not a list
def run_nlp_algorithm(documents):
    nlp_results = {}
    for cntry, doc in documents.items():
        logging.info("Working on NDC for {}".format(cntry))

        # ONLY RUNNING SUBSECTION OF DOC, first 5000 characters
        # Allows for "HTML" type
        document = types.Document(
                content=doc[:5000].encode('utf-8'),
                type=enums.Document.Type.PLAIN_TEXT)
        encoding = enums.EncodingType.UTF32

        doc_nlp_result = client.analyze_entity_sentiment(document, encoding)

        salient_entities = []
        for entity in doc_nlp_result.entities:
            # ONLY INCLUDE SALIENT ENTITIES, lowercase
            if entity.salience > SALIENCE_THRESHOLD:
                salient_entities.append(entity.name.lower())
            #doc_entities.append([entity.name, entity.salience])

        salient_entities = list(set(salient_entities))
        logging.info('salient entities in doc: {}'.format(salient_entities))
        nlp_results[cntry] = salient_entities

    return nlp_results

####
## Statistical Modeling of ML results
####

def generate_conditional_probabilities(nlp_results, corpus):
    corpus_size = len(corpus)
    logging.info('Corpus is {} words'.format(corpus_size))
    counts = np.zeros(corpus_size)
    cooccurences = np.zeros((corpus_size, corpus_size))

    for cntry, entities in nlp_results.items():
        logging.info('COND PROBS: Processing data for country {}'.format(cntry))
        noun_in_this_doc = np.zeros(corpus_size)
        for noun in entities:
            ix = corpus.index(noun)
            if noun_in_this_doc[ix] == 1:
                pass
            else:
                noun_in_this_doc[ix] = 1
                counts[ix] += 1

        cooccured = np.where(noun_in_this_doc>0)[0]
        for ix_x in cooccured:
            for ix_y in cooccured:
                cooccurences[ix_x, ix_y] += 1

    conditional_probabilities = np.nan_to_num(cooccurences / counts.T)
    logging.info('Conditional Probabilities: {}'.format(conditional_probabilities))
    return conditional_probabilities

def generate_corpus_links(conditional_probabilities, corpus):

    corpus_links = np.zeros(conditional_probabilities.shape)
    num_nouns = len(corpus)
    logging.info('Number of nouns in corpus: {}'.format(num_nouns))
    for ix_x in range(num_nouns):
        logging.info('CORPUS LINKS: Working on noun {}/{}: {}'.format(ix_x, num_nouns, corpus[ix_x]))
        for ix_y in range(ix_x):
            prob1 = conditional_probabilities[ix_x, ix_y]
            prob2 = conditional_probabilities[ix_y, ix_x]
            cond_prob = np.min((prob1, prob2))
            corpus_links[ix_x, ix_y] = cond_prob

    logging.info('Corpus links: {}'.format(corpus_links))
    return corpus_links

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
## WORKFLOW
####

if PROCESS_DATA:
    ####
    ## Load Data
    ####

    logging.info('Corpus:')
    #cmd = ['git', 'clone', 'https://github.com/mengping/ndc.git', 'data']
    #subprocess.check_output(' '.join(cmd), shell=True)
    english_ndcs = glob.glob('data/*EN.html')
    logging.info(english_ndcs)
    ndc_texts = convert_full_html_to_text(english_ndcs)
    logging.info('Number of documents: {}'.format(len(ndc_texts)))

    ####
    ## Run ML NLP Algorithm
    ####

    logging.info('Doc Results:')
    nlp_results = run_nlp_algorithm(ndc_texts)
    logging.info(nlp_results)

    with open('data/nlp_results.txt', 'w') as f:
        f.write(json.dumps(nlp_results))

    # Create list of lower case entities, sorted alphabetically
    corpus = []
    for cntry, entities in nlp_results.items():
        for entity in entities:
            corpus.append(entity)
    corpus = list(set(corpus))
    corpus.sort()

    with open('data/entities.txt', 'w') as f:
        f.write(json.dumps(corpus))

if CREATE_LINKS:
    ####
    ## Run Statistical Models to structure results
    ####

    with open('data/nlp_results.txt', 'r') as f:
        nlp_results = json.loads(f.read())
        # temporary
        for cntry, entities in nlp_results.items():
            nlp_results[cntry] = [word.lower() for word in entities]

    with open('data/entities.txt', 'r') as f:
        corpus = json.loads(f.read())
        # temporary
        corpus = [word.lower() for word in corpus]
        corpus = list(set(corpus))
        corpus.sort()

    logging.info('Conditional Probabilities:')
    conditional_probabilities = generate_conditional_probabilities(nlp_results, corpus)

    pickle.dump(conditional_probabilities, open('data/conditional_probabilities.pkl', 'wb'))

    logging.info('Corpus Links:')
    corpus_links = generate_corpus_links(conditional_probabilities, corpus)

    pickle.dump(corpus_links, open('data/corpus_links.pkl', 'wb'))


if GENERATE_COOCCURENCE_GRAPHS:

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


# # Extensions
# * Extract neighborhoods from pruned graph
# * Differentiate between proper and common nouns - highlight proper nouns to discuss the issues they care about
# * Incorporate sentiment in some measure
