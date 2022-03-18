def create_unique_constraints(conn):

    wallet_query = """CREATE CONSTRAINT UniqueAddress IF NOT EXISTS FOR (w:Wallet) REQUIRE w.address IS UNIQUE"""
    conn.query(wallet_query)

    token_query = """CREATE CONSTRAINT UniqueTokenAddress IF NOT EXISTS FOR (d:Token) REQUIRE d.address IS UNIQUE"""
    conn.query(token_query)

    space_query = """CREATE CONSTRAINT UniqueID IF NOT EXISTS FOR (d:snapshot_space) REQUIRE d.id IS UNIQUE"""
    conn.query(space_query)

    proposal_query = """CREATE CONSTRAINT UniqueID IF NOT EXISTS FOR (d:snapshot_proposal) REQUIRE d.id IS UNIQUE"""
    conn.query(proposal_query)

    strategy_query = """CREATE CONSTRAINT UniqueID IF NOT EXISTS FOR (d:snapshot_strategy) REQUIRE d.id IS UNIQUE"""
    conn.query(strategy_query)


def create_wallet_nodes(url, conn):

    wallet_node_query = f"""
                        USING PERIODIC COMMIT 2000
                        LOAD CSV WITH HEADERS FROM '{url}' AS votes
                        MERGE (w:Wallet {{address: votes.voter}})
                        return count(*)
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
                        USING PERIODIC COMMIT 10000
                        LOAD CSV WITH HEADERS FROM '{url}' AS spaces
                        MERGE(s:Snapshot:Space:snapshot_space {{id: spaces.id}})
                        set s = spaces
                    """

    conn.query(space_node_query)
    print("space nodes created")


def create_proposal_nodes(url, conn):

    proposal_node_query = f"""
                        USING PERIODIC COMMIT 10000
                        LOAD CSV WITH HEADERS FROM '{url}' AS proposals
                        MERGE(p:Snapshot:Proposal:snapshot_proposal {{id: proposals.id}})
                        MERGE (w:Wallet {{address: proposals.author}})
                        set p = proposals
                    """

    conn.query(proposal_node_query)
    print("proposal nodes created")


def create_strategy_nodes(url, conn):

    strategy_node_query = f"""
                        USING PERIODIC COMMIT 10000
                        LOAD CSV WITH HEADERS FROM '{url}' AS strategy
                        MERGE(s:Snapshot:Strategy:snapshot_strategy {{id: strategy.id}})
                        set s = strategy
                    """

    conn.query(strategy_node_query)
    print("strategy nodes created")
