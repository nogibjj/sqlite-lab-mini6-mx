Week 6: Complex SQL Query for a MySQL Database (Can be any external database including DynamoDB, Databricks, or even Neo4)

Requirements
Design a complex SQL query involving joins, aggregation, and sorting
Provide an explanation for what the query is doing and the expected results


Deliverables
data: from WNBA stats and WNBA elo data

join WNBA stats and WNBA elo data on common key ==team id

order by date

group by team

SQL query

            """SELECT team AS Team, COUNT(*) AS TotalMatchesPlayed
            FROM (SELECT tm_gms AS team FROM default.matchesdb_one
                
                SELECT * FROM default.matchesdb_one
                join 
                select * from default.wwc_matches_2_db
                on default.matchesdb_one.tm_gms = default.wwc_matches_2_db.team1

                ) AS table3
            GROUP BY Team1 
            ORDER BY data DESC;""",




