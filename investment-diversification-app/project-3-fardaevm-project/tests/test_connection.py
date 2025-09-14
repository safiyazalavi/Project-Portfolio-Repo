import neo4j
import os

os.environ["N4J_PW"] = "test1234"  # Ensure this is correct

def test_connection(ip="localhost"):
    pw = os.environ.get("N4J_PW")
    try:
        driver = neo4j.GraphDatabase.driver(uri=f"bolt://{ip}:7687", auth=("neo4j", pw))
        with driver.session() as session:
            result = session.run("RETURN 1 AS test")
            print("Connection successful:", result.single()["test"])
    except Exception as e:
        print("Connection failed:", e)

test_connection()
