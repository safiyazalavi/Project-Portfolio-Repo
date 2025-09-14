from . import *

if __name__ == '__main__':
    print("***********")
    print("Setting up graph...")

    print("Grabbing ip and opening session to Neo4J...")
    ip = os.environ.get("N4J_IP", 'fake ip')
    session = Session(ip=ip)
    print("Session established")

    print("Removing old graph if present...")
    session.drop_all()
    print("Complete")

    print("Loading ticker csv...")
    session.load_ticker_csv()
    print("Complete.")

    print("Loading correlation csv...")
    session.load_corr_csv()
    print("Complete.")

    print("Creating projection...")
    session.create_projection()
    print("Complete.")

    print("Building louvain...")
    session.build_louvain()
    print("Complete.")

    print("Resetting projection for leiden...")
    session.reset_projection_for_leiden()
    print("Complete.")

    print("Building leiden...")
    session.build_leiden()
    print("Complete.")

    print("Graph setup complete.")
    print("***********")
    