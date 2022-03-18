def create_proposal_relationships(url, conn):

    proposal_rel_query = f"""
                        USING PERIODIC COMMIT 10000
                        LOAD CSV WITH HEADERS FROM '{url}' AS proposals
                        MATCH (p:snapshot_proposal {{id: proposals.id}}), (author:Wallet {{address: proposals.author}}), (space:snapshot_space {{id: proposals.space}})
                        MERGE (author)-[:AUTHORED]->(p)
                        MERGE (space)-[:HAS_PROPOSAL]->(p)
                    """

    conn.query(proposal_rel_query)
    print("proposal relaionships created")


def create_vote_relationships(url, conn):

    vote_rel_query = f"""
                    USING PERIODIC COMMIT 10000
                    LOAD CSV WITH HEADERS FROM '{url}' as votes
                    MATCH (voter:Wallet {{address: votes.voter}}), (p:snapshot_proposal {{id: votes.proposal}})
                    MERGE (voter)-[:VOTED {{ipfs: votes.ipfs, createdAt: votes.createdAt, choice: votes.choice}}]->(p)
                    """

    conn.query(vote_rel_query)
    print("vote relationships created")
