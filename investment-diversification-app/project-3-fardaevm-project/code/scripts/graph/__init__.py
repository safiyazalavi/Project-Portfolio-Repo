from time import sleep
import neo4j
import os

### Set this in the envionment before you call the ip_session function,
# using your shell:
# $ export N4J_PW=password
# or anywhere within python code that won't leak your password:
# os.environ[OS_ENV_PW] = 'my secret password'
OS_ENV_PW = 'N4J_PW'

def ip_session(ip, db='neo4j'):
    pw = os.environ.get(OS_ENV_PW, 'fake default password')
    driver = neo4j.GraphDatabase.driver(uri="neo4j://{}:7687".format(ip), auth=("neo4j", pw))
    return driver.session(database=db)

LOAD_CORR_CSV_FMT = """
        LOAD CSV WITH HEADERS FROM '{}' AS row
        MERGE (a:Ticker {{ticker: row.a}})
        MERGE (b:Ticker {{ticker: row.b}})
        MERGE (a)-[r:REL {{weight: toFloat(row.weight), relationship: row.relationship}}]-(b)
"""

BUILD_LOUVAIN_FMT = '''
        CALL gds.louvain.stream('{}', {{ relationshipWeightProperty: 'weight',
        nodeLabels: ['*'],
        maxLevels: {},
        tolerance: {}
        }})
        YIELD nodeId, communityId
        WITH gds.util.asNode(nodeId) AS node, communityId
        SET node.community = communityId
'''

class Session(object):
    def __init__(self, ip, db='neo4j'):
        self.session = ip_session(ip, db)

    def drop_all(self):
        return self.run('match (n) detach delete n return count(n)')


    def load_ticker_csv(self, fpath='file:///user/co.csv'):
        """
        Loads Ticker nodes from CSV.

        """
        cmd = """
        LOAD CSV WITH HEADERS FROM '{}' AS row
        CREATE (n:Ticker {{
        name: row.name,
        ticker: row.ticker,
        sector: row.sector,
        industry: row.industry
        }});
        """.format(fpath)
        return self.session.run(cmd)

    def load_corr_csv(self, fpath='file:///user/pearson.csv', fmt_cmd=LOAD_CORR_CSV_FMT):
        return self.run(fmt_cmd.format(fpath))

    def remove_property(self, p='community'):
        cmd = 'MATCH (n) REMOVE n.{} RETURN count(n) AS nodesUpdated'.format(p)
        return self.session.run(cmd)

    #### Louvain
    def get_community(self):
        query = '''
        MATCH (n) return n.ticker, n.community
        '''
        return self.run(query)

    def drop_projection(self, name="stock_picker"):
        try:
            remove = """
            CALL gds.graph.drop('{}', true) yield graphName
            """.format(name)
            return self.session.run(remove)
        except Exception as e:
            print(e)


    def create_projection(self, name='stock_picker'):
        create = """
        CALL gds.graph.project('{}', 'Ticker', 'REL',
                      {{relationshipProperties: 'weight'}})
        """.format(name)
        print('calling')
        print(create)
        try:
            print(self.session.run(create).data())
        except Exception as e:
            print(e)

        print('done creating')
        sleep(1)
        return

    def reset_projection(self, name="stock_picker"):
        """
        Remove a projection by name
        Creates a projection with a Ticker.weight property on the REL relationship.

        """
        try:
            self.drop_projection(name)
        except Exception as e:
            print(f'error dropping projection: {e}')
            pass
        sleep(1)
        print('create projection')
        return self.create_projection(name)

    def build_louvain(self, levels=10, tolerance=1e-6, name='stock_picker',
                      cmd_fmt=BUILD_LOUVAIN_FMT):

        cmd = cmd_fmt.format(name, levels, tolerance)
        return self.session.run(cmd)

    def remove_property(self, p='community'):
        cmd = 'MATCH (n) REMOVE n.{} RETURN count(n) AS nodesUpdated'.format(p)
        return self.session.run(cmd)

    def load_ticker_csv(self, fpath='file:///user/co.csv'):
        cmd = f"""
        LOAD CSV WITH HEADERS FROM '{fpath}' AS row
        MERGE (n:Ticker {{
            ticker: row.ticker
        }})
        SET n.name = row.name,
            n.sector = row.sector,
            n.industry = row.industry
        """
        return self.session.run(cmd)

    def load_corr_csv(self, fpath='file:///user/pearson.csv', fmt_cmd=LOAD_CORR_CSV_FMT):
        return self.run(fmt_cmd.format(fpath))

    # Leiden
    def reset_projection_for_leiden(self, name="stock_picker"):
        remove = """
        CALL gds.graph.drop('{}', false) yield graphName
        """.format(name)
        try:
            self.run(remove)
        except Exception as e:
            print(e)

        create = """
        CALL gds.graph.project('{}', 'Ticker', {{ REL: {{ orientation: 'UNDIRECTED', properties: 'weight' }} }})
        """.format(name)
        return self.run(create)

    def build_leiden(self, name='stock_picker'):
        query = """
        CALL gds.leiden.stream('{}', {{
          relationshipWeightProperty: 'weight',
          nodeLabels: ['*'],
          gamma: 1.075,          // increased resolution parameter
          minCommunitySize: 5
        }})
        YIELD nodeId, communityId, intermediateCommunityIds
        WITH gds.util.asNode(nodeId) AS node, communityId
        SET node.leiden_community = communityId
        """.format(name)
        return self.run(query)

    # PageRank Algo
    def run_personalized_pagerank(self, source_ticker, name='stock_picker', damping_factor=0.85, iterations=20):
        """
        Runs personalized PageRank using the built-in gds.pageRank.stream procedure.
        It personalizes the computation by providing the source node via the `sourceNodes` parameter.
        The scores are stored on each node as 'personalizedPageRank' and the procedure returns (ticker, score).
        """

        node_query = f"""
        MATCH (n:Ticker {{ticker: '{source_ticker}'}})
        RETURN id(n) AS nodeId
        """

        print("Using ticker: ", source_ticker)
        result = self.session.run(node_query).single()
        if not result:
            raise ValueError(f"Ticker '{source_ticker}' not found in the graph.")
        source_node_id = result["nodeId"]

        query = f"""
        CALL gds.pageRank.stream($graph_name, {{
          maxIterations: $iterations,
          dampingFactor: $damping_factor,
          relationshipWeightProperty: 'weight',
          sourceNodes: $source_nodes
        }})
        YIELD nodeId, score
        WITH gds.util.asNode(nodeId) AS node, score
        SET node.personalizedPageRank = score
        RETURN node.ticker AS ticker, score
        ORDER BY score DESC
        """
        params = {
            "graph_name": name,
            "iterations": iterations,
            "damping_factor": damping_factor,
            "source_nodes": [source_node_id]
        }

        return self.session.run(query, params).data()

    def get_all_similar_by_ppr(self, source_ticker):
        """
        Return all tickers (excluding source_ticker) sorted descending by personalizedPageRank.
        """
        query = f"""
        MATCH (n:Ticker)
        WHERE n.personalizedPageRank IS NOT NULL AND n.ticker <> '{source_ticker}'
        RETURN n.ticker AS ticker, n.personalizedPageRank AS score
        ORDER BY score DESC
        """
        return self.session.run(query).data()

    def get_top_similar_by_ppr(self, source_ticker, top_n=10):
        """
        Runs personalized PageRank for the source ticker (if not already run) and then returns
        the top N tickers (excluding the source ticker) sorted in descending order by their personalizedPageRank scores.
        """
        self.run_personalized_pagerank(source_ticker)

        query = f"""
        MATCH (n:Ticker)
        WHERE n.personalizedPageRank IS NOT NULL AND n.ticker <> '{source_ticker}'
        RETURN n.ticker AS ticker, n.personalizedPageRank AS score
        ORDER BY score DESC
        LIMIT {top_n}
        """
        return self.session.run(query).data()

    def get_bottom_similar_by_ppr(self, source_ticker, bottom_n=5):
        """
        Returns the bottom N tickers (excluding the source ticker) sorted in ascending order by their personalizedPageRank.
        """
        query = f"""
        MATCH (n:Ticker)
        WHERE n.personalizedPageRank IS NOT NULL AND n.ticker <> '{source_ticker}'
        RETURN n.ticker AS ticker, n.personalizedPageRank AS score
        ORDER BY score ASC
        LIMIT {bottom_n}
        """
        return self.session.run(query).data()


    def count_nodes(self, name='Ticker'):
        return self.run('match (n:Ticker) return count(n)')

    def unique_property(self, prop='community'):
        q = f'MATCH (n) return distinct(n.{prop}), count(n.{prop})'
        return self.run(q)

    def get_similar(self, ticker, nproperty='community'):
        '''
        ticker: stock ticker, eg., APPL

        Return the tickers with the same `nproperty` id
        '''
        query = '''
        match (n) where n.ticker = "{ticker}"
        with n.{nproperty} as community_id
        match (m) where m.{nproperty} = community_id
        return m.ticker
        '''.format(ticker=ticker, nproperty=nproperty)
        return self.run(query).value()

    def get_groups(self, ticker, nproperty='community'):
        '''
        ticker: stock ticker, eg., APPL

        Return the tickers with the same `nproperty` id
        '''
        query = '''
        match (m)
        return m.{nproperty} as group_id, m.ticker as ticker
        '''.format(ticker=ticker, nproperty=nproperty)
        return self.run(query).values()

    def run(self, query):
        return self.session.run(query)

    def list_projections():
        return self.run('CALL gds.graph.list()').values()
