"""
Transforms and Loads data into the Azure Databricks database
"""
import os
from databricks import sql
import pandas as pd
from dotenv import load_dotenv


# load the csv file and insert into databricks
def load(dataset="db/wnba-player-stats.csv",dataset2="db/wnba-team-elo-ratings.csv"):
    """Transforms and Loads data into the Azure Databricks database"""
    df = pd.read_csv(dataset, delimiter=",", skiprows=1)
    df2 = pd.read_csv(dataset2, delimiter=",", skiprows=1)
    load_dotenv()
    server_h = os.getenv("SERVER_HOSTNAME")
    access_token = os.getenv("ACCESS_TOKEN")
    http_path = os.getenv("HTTP_PATH")
    with sql.connect(
        server_hostname=server_h,
        http_path=http_path,
        access_token=access_token,
    ) as connection:
        c = connection.cursor()
        c.execute("SHOW TABLES FROM default LIKE 'wnba-player-stats*'")
        result = c.fetchall()
        if not result:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS MatchesDB_ONE (
                    id int,
                    player_ID,
                    Player,
                    year_ID,
                    Age,
                    Tm,
                    tm_gms,
                    Tm_Net_Rtg,
                    Pos,
                    G,
                    MP,
                    MP_pct,
                    PER,
                    TS_pct,
                    ThrPAr,
                    FTr,
                    ORB_pct,
                    TRB_pct,
                    AST_pct,
                    STL_pct,
                    BLK_pct,
                    TOV_pct,
                    USG_pct,
                    OWS,
                    DWS,
                    WS,
                    WS40,
                    Composite_Rating,
                    Wins_Generated
                )
            """
            )
            # insert
            for _, row in df.iterrows():
                convert = (_,) + tuple(row)
                print(convert)
                c.execute(f"INSERT INTO MatchesDB_ONE VALUES {convert}")
        c.execute("SHOW TABLES FROM default LIKE 'wnba-team-elo-ratings*'")
        result = c.fetchall()
        # c.execute("DROP TABLE IF EXISTS WWC_MATCHES_2_DB")
        if not result:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS WWC_MATCHES_2_DB (
                    id int,
                    season,
                    date,
                    team1,
                    team2,
                    name1,
                    name2,
                    neutral,
                    playoff,
                    score1,
                    score2,
                    elo1_pre,
                    elo2_pre,
                    elo1_post,
                    elo2_post,
                    prob1,is_home1
                )
                """
            )
            for _, row in df2.iterrows():
                convert = (_,) + tuple(row)
                c.execute(f"INSERT INTO WWC_MATCHES_2_DB VALUES {convert}")
        c.close()

    return "success"

