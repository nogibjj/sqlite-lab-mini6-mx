"""
Extract a dataset from a URL like Kaggle or data.gov. 
JSON or CSV formats tend to work well
"""
import os
import requests
import pandas as pd


def extract(
    url="""
    https://raw.githubusercontent.com/fivethirtyeight/data/master/womens-world-cup-predictions/wwc-matches-20150602-093000.csv?raw=true
    """,
    url2="""
    https://raw.githubusercontent.com/fivethirtyeight/data/master/womens-world-cup-predictions/wwc-matches-20150705-205539.csv?raw=true
    """,
    file_path="data/wwc_matches_1.csv",
    file_path2="data/wwc_matches_2.csv",
    directory="data",
):
    """Extract a url to a file path"""
    if not os.path.exists(directory):
        os.makedirs(directory)
    with requests.get(url) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)
    with requests.get(url2) as r:
        with open(file_path2, "wb") as f:
            f.write(r.content)

    df = pd.read_csv(file_path)

    df_subset = df.dropna()

    df_subset.to_csv(file_path, index=False)

    df_1 = pd.read_csv(file_path2)

    df_subset_1 = df_1.dropna()

    df_subset_1.to_csv(file_path2, index=False)
    return file_path, file_path2
