####
## Import required libraries
####

from bs4 import BeautifulSoup

from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types

import numpy as np

import subprocess
import glob
import os
import logging
import sys
import json
import pickle
import base64
import re
import time
from nltk.stem import PorterStemmer
from functools import reduce
from collections import defaultdict

import requests as req

LOG_LEVEL = logging.INFO
logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)

SALIENCE_THRESHOLD = .003
### With a small co-occurence, will include more links
# and give insight into structure of the network between .10 and .30
# for many queries may want to filter by cooccurences .30 or above
MINIMUM_COOCCURENCE = .10
MINIMUM_FREQUENCY = 0

DRAW_GRAPHS = False
PROCESS_DATA = True
CREATE_LINKS = True
LOAD_NEO4J = True
REQUIRE_NEO4J_AUTH = False
MAX_TRIES = 10
STEM_NOUNS = False

####
## Neo4j Endpoints
####
NEO4J_API='http://climatewatchndcnlp-neo4j-1.inst.f3fe33bf-8737-e28a-901a-c1d32b1a5ec2.us-east-1.triton.zone:7474/'
NEO4J_AUTH = NEO4J_API+'user/neo4j'

NEO4J_NODE = NEO4J_API+'db/data/node/{node_id}'
NEO4J_CREATE_NODE = NEO4J_API+'db/data/node'
NEO4J_LABEL_NODE = NEO4J_API+'db/data/node/{node_id}/labels'

NEO4J_LINKS = NEO4J_API+'db/data/relationships'
NEO4J_CREATE_LINK = NEO4J_API+'db/data/node/{node_id}/relationships'

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

###
## FILTERS
###
def only_letters(entity, prog):
    noun = entity.name
    #logging.debug('Name: {}'.format(noun))
    match_q = bool(prog.match(noun))
    #logging.debug('Matches pattern of letters and spaces: {}'.format(match_q))
    return match_q

def is_salient(entity):
    #logging.debug('Echo from chamber of salience')
    return entity.salience > SALIENCE_THRESHOLD

def build_doc_count(agg, noun):
    agg[noun]+=1
    return agg

def build_intermediate_count(agg, noun_tuple):
    agg[noun_tuple[0]] += noun_tuple[1]
    return agg

def build_total_count(agg,noun_count):
    return reduce(build_intermediate_count, noun_count.items(), agg)

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

        # NLP API
        doc_nlp_result = client.analyze_entity_sentiment(document, encoding)

        filtered_results1 = list(filter(is_salient , doc_nlp_result.entities))
        #logging.debug('Salient entities in doc: {}'.format(filtered_results1))
        #logging.debug('Type of salient resutls: {}'.format(filtered_results1))

        prog = re.compile('^[A-Za-z ]+$')
        filtered_results2 = list(filter(lambda entity: only_letters(entity, prog) , filtered_results1))
        #logging.debug('Filter for only letters: {}'.format(filtered_results2))

        # filtered_results = filter(some_other_func, filtered_results)

        lower_case_nouns = sorted(map(lambda entity: entity.name.lower(), filtered_results2))
        logging.debug('Lower case names: {}'.format(lower_case_nouns))

        if STEM_NOUNS:
            stemmer = PorterStemmer()
            stemmed_nouns = list(map(stemmer.stem, lower_case_nouns))
            noun_count = dict(reduce(build_doc_count, stemmed_nouns, defaultdict(int)))
        else:
            noun_count = dict(reduce(build_doc_count, lower_case_nouns, defaultdict(int)))

        logging.info('Unique nouns: {}'.format(noun_count))
        nlp_results[cntry] = noun_count

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






####
## Creating a Graph Using Neo4j
####

def neo4j_authenticate(_user='neo4j', _pass='neo4j'):
    res = req.get(NEO4J_AUTH, headers = createHeaders(_user, _pass))
    return res

def createHeaders(_user='neo4j', _pass='neo4j'):
    pass_string = '{}:{}'.format(_user, _pass)
    base64userpass = base64.b64encode(pass_string.encode('utf-8'))
    return {
        'Content-Type':'application/json',
        'Authorization':'Basic {}'.format(base64userpass)
    }

def create_noun_nodes(nouns, nodes={}, _user='neo4j', _pass='neo4j'):
    num_nouns = len(nouns)
    for ix, noun in enumerate(nouns):
        if noun not in nodes:
            # Create node
            logging.info('Creating node for noun {}/{}: {}'.format(ix, num_nouns, noun))
            node_props = {
                'noun':noun
            }
            node_info = req.post(NEO4J_CREATE_NODE,
                            data=json.dumps(node_props),
                            headers=createHeaders(_user, _pass)).json()

            # Add label
            node_id = node_info['metadata']['id']
            req.post(NEO4J_LABEL_NODE.format(node_id=node_id),
                    data=json.dumps('Noun'),
                    headers=createHeaders(_user, _pass))

            nodes[noun] = node_info
    return nodes

def create_country_nodes(countries, nodes={}, _user='neo4j', _pass='neo4j'):
    num_countries = len(countries)
    for ix, country in enumerate(countries):
        if country not in nodes:
            logging.info('Creating node for noun {}/{}: {}'.format(ix, num_countries, country))
            node_props = {
                'country':country
            }
            node_info = req.post(NEO4J_CREATE_NODE,
                            data=json.dumps(node_props),
                            headers=createHeaders(_user, _pass)).json()

            # Add label
            node_id = node_info['metadata']['id']
            req.post(NEO4J_LABEL_NODE.format(node_id=node_id),
                    data=json.dumps('Country'),
                    headers=createHeaders(_user, _pass))

            nodes[country] = node_info
    return nodes

# https://graphaware.com/neo4j/2013/10/11/neo4j-bidirectional-relationships.html
def create_relationship(nodeA, nodeB, _type, _data, _user='neo4j', _pass='neo4j'):
    relationship_info = {
          "to" : NEO4J_NODE.format(node_id=nodeB),
          "type" : _type,
          "data" : _data
    }
    res = req.post(NEO4J_CREATE_LINK.format(node_id=nodeA),
                data=json.dumps(relationship_info),
                headers=createHeaders(_user, _pass))
    return res

def create_graph(nlp_results, corpus, corpus_links):
    country_nodes = create_country_nodes(nlp_results)
    noun_nodes = create_noun_nodes(corpus)

    # Create Country-Noun Links
    for country, noun_count in nlp_results.items():
        for noun in noun_count:
            logging.info('NEO4J: NOUN GRAPH: Making connections for country: {}'.format(country))
            nodeA = country_nodes[country]['metadata']['id']
            nodeB = noun_nodes[noun]['metadata']['id']
            # TO DO: update ML step to count frequency of mentions
            # of each entity in a country NDC
            frequency = nlp_results[country][noun]
            if frequency > MINIMUM_FREQUENCY:
                link_data = {
                    'frequency':frequency
                }
                evidence = create_relationship(nodeA, nodeB, 'mention', link_data)
                logging.debug('Evidence from relationship creation: {}'.format(evidence.text))

    # Create Noun-Noun Links
    num_noun_nodes = len(corpus)
    for ix_x in range(num_noun_nodes):
        logging.info('NEO4J: NOUN GRAPH: Making connections for noun {}/{}: {}'.format(ix_x, num_noun_nodes, corpus[ix_x]))
        for ix_y in range(ix_x):
            nounA = corpus[ix_x]
            nounB = corpus[ix_y]
            nodeA = noun_nodes[nounA]['metadata']['id']
            nodeB = noun_nodes[nounB]['metadata']['id']
            link = corpus_links[ix_x, ix_y]
            if link > MINIMUM_COOCCURENCE:
                link_data = {
                    'probability':link
                }
                evidence = create_relationship(nodeA, nodeB, 'cooccurrence', link_data)
                logging.debug('Evidence from relationship creation: {}'.format(evidence.text))

    return country_nodes, noun_nodes






####
## WORKFLOW
####
def main():
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

        ####
        ## Tally results
        ####

        with open('data/nlp_results.txt', 'w') as f:
            f.write(json.dumps(nlp_results))

        corpus_count = dict(reduce(build_total_count, nlp_results.values(), defaultdict(int)))
        corpus = sorted(corpus_count.keys())

        with open('data/corpus.txt', 'w') as f:
            f.write(json.dumps(corpus))

        with open('data/corpus_count.txt', 'w') as f:
            f.write(json.dumps(corpus_count))

    if CREATE_LINKS:
        ####
        ## Run Statistical Models to structure results
        ####

        with open('data/nlp_results.txt', 'r') as f:
            nlp_results = json.loads(f.read())

        with open('data/corpus.txt', 'r') as f:
            corpus = json.loads(f.read())

        logging.info('Conditional Probabilities:')
        conditional_probabilities = generate_conditional_probabilities(nlp_results, corpus)

        pickle.dump(conditional_probabilities, open('data/conditional_probabilities.pkl', 'wb'))

        logging.info('Corpus Links:')
        corpus_links = generate_corpus_links(conditional_probabilities, corpus)

        pickle.dump(corpus_links, open('data/corpus_links.pkl', 'wb'))



    if LOAD_NEO4J:

        if REQUIRE_NEO4J_AUTH:
            logging.info("AUTHENTICATING TO NEO4J")
            neo4j_authenticate()

        logging.info("LOADING DATA")
        corpus_links = pickle.load(open('data/corpus_links.pkl', 'rb'))

        with open('data/corpus.txt', 'r') as f:
            corpus = json.loads(f.read())

        with open('data/nlp_results.txt', 'r') as f:
            nlp_results = json.loads(f.read())

        # TO DO: Fetch all existing nodes to not uploda them twice
        #existing_nodes =

        logging.info("CREATING GRAPH")
        flag = True
        count = 0
        while flag:
            try:
                country_nodes, noun_nodes = create_graph(nlp_results, corpus, corpus_links)
                flag = False
            except:
                time.sleep(5)
                count += 1
                if count < MAX_TRIES:
                    logging.error('Neo4j server not yet ready, trying again')
                else:
                    logging.error('Not trying any more.')
                    flag = False
                    raise Exception('Could not reach Neo4j server after {} attempts'.format(MAX_TRIES))

        logging.info("SAVING NODES")
        with open('data/country_nodes.txt', 'w') as f:
            f.write(json.dumps(country_nodes))

        with open('data/noun_nodes.txt', 'w') as f:
            f.write(json.dumps(noun_nodes))

    logging.info("FINISHED")
    # # Extensions
    # * Extract neighborhoods from pruned graph
    # * Differentiate between proper and common nouns - highlight proper nouns to discuss the issues they care about
    # * Incorporate sentiment in some measure


    ####
    ## CYPHER QUERIES
    ####

    # MATCH (c:Country { country: 'SOM' })-->(n:Noun) RETURN c, n
    ## TO DO: how to only show connections over a certain probability?

    # 50 most relevant links, ranked by the
