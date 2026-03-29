import tempfile
import webbrowser

from tqdm import tqdm
from pyvis.network import Network
import networkx as nx
import matplotlib.pyplot as plt


def pyvis_graph(
    nodes: list,
    edges: list,
) -> None:

    net = Network(
        height="100vh",
        width="100%",
        directed=True,
        cdn_resources="remote"
    )

    print("Adding nodes")
    for n in tqdm(nodes):
        net.add_node(n)
    print("Adding edges")
    for e in tqdm(edges):
        net.add_edge(e[0], e[1])

    with tempfile.NamedTemporaryFile(suffix=".html") as f:
        html_path = f.name
    net.write_html(html_path)
    webbrowser.open(f"file://{html_path}")
    


def networkx_graph(nodes: list, edges: list) -> None:

    G = nx.Graph()

    for n in nodes:
        G.add_node(n)
    for e in edges:
        G.add_edge(e[0], e[1])

    pos = nx.kamada_kawai_layout(G) 

    plt.figure(figsize=(10, 10))

    nx.draw(
        G,
        pos,
        node_size=5,
        with_labels=False,
        edge_color="black",
        width=0.1
    )

    plt.savefig("graph.svg", dpi=300)
    plt.close()
