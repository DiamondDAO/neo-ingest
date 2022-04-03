def create_unique_constraints(conn):

    wallet_query = """CREATE CONSTRAINT UniqueAddress IF NOT EXISTS FOR (w:Wallet) REQUIRE w.address IS UNIQUE"""
    conn.query(wallet_query)

    token_query = """CREATE CONSTRAINT UniqueTokenAddress IF NOT EXISTS FOR (d:Token) REQUIRE d.address IS UNIQUE"""
    conn.query(token_query)

    space_query = """CREATE CONSTRAINT UniqueID IF NOT EXISTS FOR (d:Space) REQUIRE d.id IS UNIQUE"""
    conn.query(space_query)
    # it's fine if we have DAOhaus :Proposal and Snapshot :Proposal labels be the same index because the two ids are very different
    proposal_query = """CREATE CONSTRAINT UniqueID IF NOT EXISTS FOR (d:Proposal) REQUIRE d.id IS UNIQUE"""
    conn.query(proposal_query)

    # we will most likely never search for a specific strategy so no need to index this
    # strategy_query = """CREATE CONSTRAINT UniqueID IF NOT EXISTS FOR (d:Strategy) REQUIRE d.id IS UNIQUE"""
    # conn.query(strategy_query)

# removed the return count(*)
def create_wallet_nodes(url, conn):

    wallet_node_query = f"""
                        USING PERIODIC COMMIT 2000
                        LOAD CSV WITH HEADERS FROM '{url}' AS votes
                        MERGE (w:Wallet {{address: votes.voter}})
                        """

    conn.query(wallet_node_query)
    print("wallet nodes created")


def create_token_nodes(url, conn):

    token_node_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS tokens
                        MERGE(t:Token {{address: tokens.address}})
                        ON CREATE set t = tokens
                    """

    conn.query(token_node_query)
    print("token nodes created")


def create_space_nodes(url, conn):

    space_node_query = f"""
                        USING PERIODIC COMMIT 1000
                        LOAD CSV WITH HEADERS FROM '{url}' AS spaces
                        MERGE(s:Snapshot:Space {{id: spaces.id}})
                        ON CREATE set s = spaces
                    """

    conn.query(space_node_query)
    print("space nodes created")

# added datetime
def create_proposal_nodes(url, conn):

    proposal_node_query = f"""
                        USING PERIODIC COMMIT 1000
                        LOAD CSV WITH HEADERS FROM '{url}' AS proposals
                        MERGE(p:Snapshot:Proposal {{id: proposals.id}})
                        ON CREATE set p = proposals,
                        p.createdAt = datetime(apoc.date.toISO8601(toInteger(proposals.createdAt), 's')),
                        p.start = datetime(apoc.date.toISO8601(toInteger(proposals.start), 's')),
                        p.end = datetime(apoc.date.toISO8601(toInteger(proposals.end), 's'))
                        MERGE (w:Wallet {{address: proposals.author}})
                    """

    conn.query(proposal_node_query)
    print("proposal nodes created")


def create_strategy_nodes(url, conn):

    strategy_node_query = f"""
                        USING PERIODIC COMMIT 1000
                        LOAD CSV WITH HEADERS FROM '{url}' AS strategy
                        MERGE(s:Snapshot:Strategy {{id: strategy.id}})
                        ON CREATE set s = strategy
                    """

    conn.query(strategy_node_query)
    print("strategy nodes created")
