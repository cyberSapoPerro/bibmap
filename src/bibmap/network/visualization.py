import networkx as nx
import matplotlib.pyplot as plt


def nx_graph(nodes: list[str], edges: list[tuple[str, str]]) -> None:
    """Generate a static NetworkX graph and save as PDF.

    Args:
        nodes: List of node identifiers (DOIs).
        edges: List of (source, target) tuples representing citation edges.
    """
    G = nx.Graph()

    for n in nodes:
        G.add_node(n)
    for e in edges:
        G.add_edge(e[0], e[1])

    pos = nx.kamada_kawai_layout(G)

    plt.figure(figsize=(10, 10))

    nx.draw(G, pos, node_size=1, with_labels=False, edge_color="black", width=0.1)

    plt.savefig("graph.pdf", dpi=300)
    plt.close()
