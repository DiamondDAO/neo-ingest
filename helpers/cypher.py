def create_token_nodes(url, conn):
    
    unique_query = '''CREATE CONSTRAINT UniqueTokenAddress IF NOT EXISTS FOR (d:Token) REQUIRE d.address IS UNIQUE'''
    
    conn.query(unique_query)
    
    token_node_query = f'''
                        LOAD CSV WITH HEADERS FROM '{url}' AS tokens
                        MERGE(t:Token {{address: tokens.address}})
                        set t = tokens
                    '''

    conn.query(token_node_query)
    print("token nodes created")
    
def create_member_nodes(url, conn):
    
    member_node_query = f'''
                        LOAD CSV WITH HEADERS FROM '{url}' AS members
                        MERGE(t:Member:Daohaus {{address: members.address, daoAddress: members.daoAddress}})
                        set t = members
                    '''
    conn.query(member_node_query)
    print("member nodes created")
    
def create_dao_nodes(url, conn):
    
    unique_query = '''CREATE CONSTRAINT UniqueAddressId IF NOT EXISTS FOR (d:Dao) REQUIRE d.address IS UNIQUE'''
    
    conn.query(unique_query)
    
    dao_node_query = f'''
                        LOAD CSV WITH HEADERS FROM '{url}' AS daos
                        MERGE(d:Dao:Daohaus:Entity {{address: daos.address}})
                        set d = daos
                    '''
    conn.query(dao_node_query)
    print("dao nodes created")
    
def create_proposal_nodes(url, conn):
    
    unique_query = '''CREATE CONSTRAINT UniqueProposalId IF NOT EXISTS FOR (d:Proposal) REQUIRE d.id IS UNIQUE'''
    
    conn.query(unique_query)
    
    proposal_node_query = f'''
                        LOAD CSV WITH HEADERS FROM '{url}' AS proposals
                        MERGE(d:Proposal:Daohaus {{id: proposals.id}})
                        set d = proposals
                    '''
    conn.query(proposal_node_query)
    print("proposal nodes created")