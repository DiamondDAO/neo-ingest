def create_token_nodes(url, conn):

    unique_query = """CREATE CONSTRAINT UniqueTokenAddress IF NOT EXISTS FOR (d:Token) REQUIRE d.address IS UNIQUE"""

    conn.query(unique_query)

    token_node_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS tokens
                        MERGE(t:Token {{address: tokens.address}})
                        set t = tokens
                    """

    conn.query(token_node_query)
    print("token nodes created")


def create_member_nodes(url, conn):

    member_node_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS members
                        MERGE(t:Member:Daohaus {{address: members.address, daoAddress: members.daoAddress}})
                        set t = members
                    """
    conn.query(member_node_query)
    print("member nodes created")


def create_dao_nodes(url, conn):

    unique_query = """CREATE CONSTRAINT UniqueAddressId IF NOT EXISTS FOR (d:Dao) REQUIRE d.address IS UNIQUE"""

    conn.query(unique_query)

    dao_node_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS daos
                        MERGE(d:Dao:Daohaus:Entity {{address: daos.address}})
                        set d = daos
                    """
    conn.query(dao_node_query)
    print("dao nodes created")


def create_proposal_nodes(url, conn):

    unique_query = """CREATE CONSTRAINT UniqueProposalId IF NOT EXISTS FOR (d:Proposal) REQUIRE d.id IS UNIQUE"""

    conn.query(unique_query)

    proposal_node_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS proposals
                        MERGE(d:Proposal:Daohaus {{id: proposals.id}})
                        set d = proposals
                    """
    conn.query(proposal_node_query)
    print("proposal nodes created")


def create_dao_relationships(url, conn):
    relationship_query = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' AS row
                            MATCH (dao:Daohaus:Dao {{address: row.dao}}), (summoner:Daohaus:Member {{address: row.summoner, daoAddress: row.dao}}), (t:Token {{address: row.depositToken}})
                            MERGE (dao)-[:HAS_DEPOSIT_TOKEN]->(t)
                            MERGE (summoner)-[:IS_SUMMONER]->(dao)
                            
                            """

    conn.query(relationship_query)
    print("dao relationships created")


def create_approved_token_relationships(url, conn):
    relationship_query = f"""
                         LOAD CSV WITH HEADERS FROM '{url}' as row
                         MATCH (dao:Daohaus:Dao {{address: row.dao}}), (t:Token {{address: row.approvedToken}})
                         MERGE (dao)-[:HAS_APPROVED_TOKEN]->(t)
                        """

    conn.query(relationship_query)
    print("approved token relationships created")


def create_member_relationships(url, conn):
    relationship_query = f"""
                         LOAD CSV WITH HEADERS FROM '{url}' as row
                         MATCH (d:Daohaus:Dao {{address: row.dao}}), (m:Daohaus:Member {{address: row.member, daoAddress: row.dao}})
                         MERGE (m)-[r:IS_MEMBER]->(d)
                        """

    conn.query(relationship_query)
    print("member relationships created")


def create_proposal_relationships(url, conn):
    relationship_query = f"""
                         LOAD CSV WITH HEADERS FROM '{url}' as row
                         MATCH (p:Daohaus:Proposal {{id: row.proposal}}), (app:Daohaus:Member {{address: row.applicant, daoAddress: row.dao}}), (prop:Daohaus:Member {{address: row.proposer, daoAddress: row.dao}}),
                         (spon:Daohaus:Member {{address: row.sponsor, daoAddress: row.dao}}), (proc:Daohaus:Member {{address: row.processor, daoAddress: row.dao}}), (d:Daohaus:Dao {{address: row.dao}})
                         MERGE (d)-[:HAS_PROPOSAL]->(p)
                         MERGE (spon)-[:SPONSORED]->(p)
                         MERGE (prop)-[:PROPOSED]->(p)
                         MERGE (proc)-[:PROCESSED]->(p)
                         MERGE (app)-[:IS_APPLICANT]->(p)
                        """

    conn.query(relationship_query)
    print("proposal relationships created")


def create_vote_relationships(url, conn):
    relationship_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as row
                        MATCH (p:Daohaus:Proposal {{id: row.propsal}}), (voter:Daohaus:Member {{address: row.member, daoAddress: row.dao}})
                        MERGE (voter)-[:VOTED {{createdAt: row.createdAt, vote: row.vote}}]->(p)
                        """

    conn.query(relationship_query)
    print("vote relationships created")
