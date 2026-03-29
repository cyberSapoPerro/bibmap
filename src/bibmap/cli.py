from db.mannager import set_db_connection
import db.querys as querys
from network.visualization import networkx_graph

TEST_DOI = "10.1103/physreve.87.032113"


def main():
    conn = set_db_connection()
    nodes = querys.fetch_paper_dois(conn)
    edges = querys.fetch_citation_edges_for_nodes(conn, nodes)
    final_nodes = querys.collect_nodes_from_edges(edges)
    networkx_graph(final_nodes, edges)


if __name__ == "__main__":
    main()
