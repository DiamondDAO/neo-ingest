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

    {
        "name": "multichain",
        "params": {
            "graphs": {
                "56": "https://api.thegraph.com/subgraphs/name/apyvision/block-info",
                "137": "https://api.thegraph.com/subgraphs/name/sameepsi/maticblocks",
            },
            "symbol": "MULTI",
            "strategies": [
                {
                    "name": "erc20-balance-of",
                    "params": {"address": "0x1C7ede23b1361acC098A1e357C9085D131b34a01", "decimals": 18},
                    "network": "1",
                },
                {
                    "name": "erc20-balance-of",
                    "params": {"address": "0x53D76f967De13E7F95e90196438DCe695eCFA957", "decimals": 18},
                    "network": "137",
                },
            ],
        },
    }

