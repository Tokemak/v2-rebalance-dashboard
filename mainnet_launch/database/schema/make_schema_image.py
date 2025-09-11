from __future__ import annotations


import pydot
from sqlalchemy_schemadisplay import create_schema_graph

from mainnet_launch.database.schema.full import ENGINE, Base


def make_schema_image():
    # 1) Build the base ERD graph
    graph = create_schema_graph(
        engine=ENGINE,
        metadata=Base.metadata,
        show_datatypes=False,
        show_indexes=False,
        rankdir="LR",
    )

    # 2) Apply your global styling
    graph.set_graph_defaults(
        splines="ortho",
        nodesep="0.6",
        ranksep="0.75",
        fontsize="12",
        dpi="300",
    )
    graph.set_node_defaults(
        shape="rectangle",
        style="filled",
        fillcolor="#f9f9f9",
        fontname="Helvetica",
    )
    graph.set_edge_defaults(
        color="#555555",
        arrowsize="0.7",
    )

    # 3) Define clusters for logical groups of tables
    #    Each cluster is a pydot.Subgraph whose name begins with "cluster_"
    autopool_tables = [
        "autopools",
        "autopool_states",
        "autopool_deposit",
        "autopool_withdrawal",
        "autopool_fees",
        "autopool_destination_states",
    ]
    dest_tables = [
        "destinations",
        "destination_states",
        "destination_tokens",
        "destination_token_values",
    ]
    rebalance_tables = ["rebalance_plan", "rebalance_events", "solver_profit"]

    def make_cluster(name, label, table_names):
        # 1) Create a true Graphviz cluster subgraph
        sub = pydot.Cluster(
            graph_name=f"cluster_{name}",  # must start with "cluster_"
            label=label,  # cluster box title
            bgcolor="lightgrey",  # cluster background color
            style="dashed",  # dashed border style
        )

        # 2) Add each table as a node (by name)
        for tbl in table_names:
            sub.add_node(pydot.Node(tbl))

        return sub

    graph.add_subgraph(make_cluster("autopool", "Autopools", autopool_tables))
    graph.add_subgraph(make_cluster("dest", "Destinations", dest_tables))
    graph.add_subgraph(make_cluster("rebalance", "Rebalance", rebalance_tables))

    # 4) Render to a high-res PNG
    graph.write("mainnet_launch/database/schema/schema.png", format="png")
    print("Wrote mainnet_launch/database/schema/schema.png")


if __name__ == "__main__":
    make_schema_image()
