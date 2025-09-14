from scripts.graph import Session

def load_mst_graph():
    sess = Session(ip="localhost")
    # Link to neo4j location of pearson_mst
    sess.load_corr_csv("http://localhost:11001/project-7cab259a-ecef-4c5c-a077-28bad519bdf8/pearson_mst.csv")
    print("MST graph loaded.")

if __name__ == "__main__":
    load_mst_graph()