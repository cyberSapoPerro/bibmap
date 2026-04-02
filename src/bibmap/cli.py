import argparse

from halo import Halo

from bibmap.db.ingestion import enrich_graph_dois
from bibmap.db.manager import set_db_connection
from bibmap.db.queries import fetch_citation_graph_data
from bibmap.network.visualization import nx_graph


TEST_DOI = "10.1103/physreve.87.032113"


def main():
    parser = argparse.ArgumentParser(
        prog="bibmap",
        description="Bibliographic graph exploration tool",
    )
    parser.add_argument(
        "--doi",
        default=TEST_DOI,
        help="Root DOI to explore (default: %(default)s)",
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=1,
        help="Graph traversal depth (default: %(default)s)",
    )
    parser.add_argument(
        "--enrich",
        action="store_true",
        help="Enrich papers with metadata before visualization",
    )
    parser.add_argument(
        "--renderer",
        choices=["nx"],
        default="nx",
        help="Visualization renderer (default: %(default)s)",
    )

    args = parser.parse_args()

    conn = set_db_connection()
    print(f"\n{'=' * 60}")
    print("  bibmap - Bibliographic Graph Explorer")
    print(f"{'=' * 60}\n")

    print(f"Fetching citation graph for {args.doi}...")
    nodes, edges = fetch_citation_graph_data(conn, args.doi, depth=args.depth)

    print("  📊 Graph Statistics:")
    print(f"     • Nodes: {len(nodes)}")
    print(f"     • Edges: {len(edges)}")
    print(f"     • Depth: {args.depth}\n")

    if args.enrich:
        enrich_graph_dois(conn, nodes)
        print("  ✅ Database enriched\n")

    print("Generating visualization...")
    spinner = Halo(spinner="dots")
    spinner.start()
    nx_graph(nodes, edges)
    spinner.stop()
    print("  ✅ Visualization generated\n")


if __name__ == "__main__":
    main()
