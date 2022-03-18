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

from snapshot.helpers.cypher_relationships import *
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

    # create proposal relationships
    proposal_df = read_df_from_s3(BUCKET, "neo/snapshot/raw/proposal.csv")
    json_proposal = proposal_df.to_dict("records")

    proposal_rels = []
    for item in json_proposal:
        current_dict = {}
        current_dict["proposal"] = item["id"]
        current_dict["author"] = item["author"]
        current_dict['space'] = item['space']
        proposal_rels.append(current_dict)

    primary_proposal_relationship_df = pd.DataFrame(proposal_rels)
    print(f"Proposals: {len(primary_proposal_relationship_df)}")
    url = write_df_to_s3(
        primary_proposal_relationship_df, BUCKET, "neo/daohaus/relationships/proposal.csv", resource, s3
    )
    create_proposal_relationships(url, conn)
    set_object_private(BUCKET, "neo/daohaus/relationships/proposal.csv", resource)

    # create vote relationships
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
    print(f"Votes: {len(vote_df)}")

    SPLIT_SIZE = os.environ.get("SPLIT_SIZE", 20000)
    SPLIT_SIZE = int(SPLIT_SIZE)

    list_vote_chunks = split_dataframe(vote_df, SPLIT_SIZE)
    for idx, vote_batch in enumerate(list_vote_chunks):
        url = write_df_to_s3(vote_batch, BUCKET, f"neo/snapshot/relationships/votes/vote-{idx * SPLIT_SIZE}.csv", resource, s3)
        create_vote_relationships(url, conn)
        set_object_private(BUCKET, f"neo/snapshot/relationships/votes/vote-{idx * SPLIT_SIZE}.csv", resource)
        print(idx * SPLIT_SIZE)
        

