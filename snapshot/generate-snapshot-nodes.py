import boto3
from datetime import datetime
import json
from neo4j import GraphDatabase
import pandas as pd
import s3fs
from dotenv import load_dotenv
import numpy as np
import os
import sys

sys.path.append(".")

from snapshot.helpers.cypher import *
from helpers.s3 import *
from helpers.graph import ChainverseGraph


if __name__ == "__main__":

    load_dotenv()
    uri = os.getenv("NEO_URI")
    username = os.getenv("NEO_USERNAME")
    password = os.getenv("NEO_PASSWORD")
    conn = ChainverseGraph(uri, username, password)

    resource = boto3.resource("s3")
    s3 = boto3.client("s3")
    BUCKET = "chainverse"

    # create proposal nodes
    content_object = s3.get_object(Bucket="chainverse", Key="snapshot/proposals/01-07-2022/proposals.json")
    data = content_object["Body"].read().decode("utf-8")
    json_data = json.loads(data)

    proposal_list = []
    for entry in json_data:
        current_dict = {}
        current_dict["id"] = entry["id"]
        current_dict["ipfs"] = entry["ipfs"]
        current_dict["author"] = entry["author"]
        current_dict["createdAt"] = entry["created"]
        current_dict["network"] = entry["network"]
        current_dict["type"] = entry["type"]

        current_dict["title"] = entry["title"].replace('"', "").replace("'", "").replace("\\", "").strip()
        current_dict["body"] = entry["body"].replace('"', "").replace("'", "").replace("\\", "").strip()

        choices = json.dumps(entry["choices"])
        choices = choices.replace('"', "").replace("'", "").strip()

        current_dict["choices"] = choices
        current_dict["start"] = entry["start"]
        current_dict["end"] = entry["end"]
        current_dict["snapshot"] = entry["snapshot"]
        current_dict["state"] = entry["state"]
        current_dict["link"] = entry["link"].strip()

        proposal_list.append(current_dict)

    proposal_df = pd.DataFrame(proposal_list)
    print("Proposal Nodes: ", len(proposal_df))

    proposal_df["createdAt"].fillna(-1, inplace=True)
    proposal_df["type"].fillna(-1, inplace=True)
    proposal_df["title"].fillna("", inplace=True)
    proposal_df["body"].fillna("", inplace=True)
    proposal_df["choices"].fillna("", inplace=True)
    proposal_df["start"].fillna(-1, inplace=True)
    proposal_df["end"].fillna(-1, inplace=True)
    proposal_df["state"].fillna("", inplace=True)
    proposal_df["link"].fillna("", inplace=True)
    proposal_df["snapshot"].fillna(-1, inplace=True)
    url = write_df_to_s3(proposal_df, BUCKET, "neo/snapshot/nodes/proposal.csv", resource, s3)
    create_proposal_nodes(url, conn)
    set_object_private(BUCKET, "neo/snapshot/nodes/proposal.csv", resource)

    # create space nodes
    content_object = s3.get_object(Bucket="chainverse", Key="snapshot/spaces/01-07-2022/spaces.json")
    data = content_object["Body"].read().decode("utf-8")
    json_data = json.loads(data)

    space_list = []
    strategy_list = []
    for entry in json_data:
        current_dict = {}
        current_dict["id"] = entry["id"]
        current_dict["name"] = entry["name"]
        current_dict["about"] = entry["about"].replace('"', "").replace("'", "").replace("\\", "").strip()
        current_dict["network"] = entry["network"]

        try:
            current_dict["avatar"] = entry["avatar"]
        except:
            current_dict["avatar"] = ""

        try:
            current_dict["minScore"] = entry["filters"]["minScore"]
        except:
            current_dict["minScore"] = -1

        try:
            current_dict["onlyMembers"] = entry["filters"]["onlyMembers"]
        except:
            current_dict["onlyMembers"] = False

        try:
            current_dict["symbol"] = entry["symbol"]
        except:
            current_dict["symbol"] = ""

        for strategy in entry["strategies"]:
            strategy_list.append({"space": entry["id"], "strategy": strategy})

        space_list.append(current_dict)

    space_df = pd.DataFrame(space_list)
    print("Space nodes: ", len(space_df))
    url = write_df_to_s3(space_df, BUCKET, "neo/snapshot/nodes/space.csv", resource, s3)
    create_space_nodes(url, conn)
    set_object_private(BUCKET, "neo/snapshot/nodes/space.csv", resource)

    strategy_relationships = []
    token_list = []
    for item in strategy_list:
        current_dict = {}
        space = item.get("space", "")
        if space == "":
            continue
        current_dict["space"] = space

        entry = item.get("strategy", "")
        if entry == "":
            continue

        try:
            token_dict = {}
            params = entry.get("params", "")
            if params == "":
                continue
            address = params.get("address", "")
            if address == "" or not isinstance(address, str):
                continue
            token_dict["address"] = address
            token_dict["symbol"] = params.get("symbol", "")
            token_dict["decimals"] = params.get("decimals", -1)
            current_dict["token"] = token_dict["address"]
            token_list.append(token_dict)
            strategy_relationships.append(current_dict)
        except:
            continue

    print(token_list[0])
    token_df = pd.DataFrame(token_list)
    token_df.drop_duplicates(subset="address", inplace=True)
    print("Token nodes: ", len(token_df))
    url = write_df_to_s3(token_df, BUCKET, "neo/snapshot/nodes/token.csv", resource, s3)
    create_token_nodes(url, conn)
    set_object_private(BUCKET, "neo/snapshot/nodes/token.csv", resource)

    strategy_df = pd.DataFrame(strategy_relationships)
    write_df_to_s3(strategy_df, BUCKET, "neo/snapshot/relationships/strategy.csv", resource, s3, "private")

    # get vote nodes
    content_object = s3.get_object(Bucket="chainverse", Key="snapshot/votes/01-07-2022/votes.json")
    data = content_object["Body"].read().decode("utf-8")
    json_data = json.loads(data)

    vote_list = []
    for entry in json_data:
        current_dict = {}
        current_dict["id"] = entry["id"]
        current_dict["voter"] = entry["voter"]
        current_dict["createdAt"] = entry["created"]
        current_dict["ipfs"] = entry["ipfs"]

        try:
            current_dict["choice"] = entry["choice"]
            current_dict["proposal"] = entry["proposal"]["id"]
            current_dict["space"] = entry["space"]["id"]
        except:
            print("xxx")
            continue

        vote_list.append(current_dict)

    vote_df = pd.DataFrame(vote_list)
    vote_df.drop_duplicates("id", inplace=True)
    print(len(vote_df))

    SPLIT_SIZE = os.environ.get("SPLIT_SIZE", 20000)
    SPLIT_SIZE = int(SPLIT_SIZE)
    
    list_vote_chunks = split_dataframe(vote_df, SPLIT_SIZE)
    for idx, vote_batch in enumerate(list_vote_chunks):
        url = write_df_to_s3(vote_batch, BUCKET, f"neo/snapshot/nodes/votes/vote-{idx * SPLIT_SIZE}.csv", resource, s3)
        create_wallet_nodes(url, conn)
        set_object_private(BUCKET, f"neo/snapshot/nodes/votes/vote-{idx * SPLIT_SIZE}.csv", resource)
        print(idx * SPLIT_SIZE)

    # add labels for vote nodes to speed up initial query
    vote_label_query = f"""
                        CALL apoc.periodic.iterate(
                        "MATCH (n:snapshot_vote) return n",
                        "set n:Snapshot:Vote",
                        {{batchsize: 5000, parallel:true}});
                        """

    conn.query(vote_label_query)
    print("vote labels created")
