"""Visualize lineage as a directed graph centered on one table.

Outputs:
- /app/data/lineage_graph.png — the graph image
- /app/data/lineage_graph.dot — graphviz DOT source (for prettier external rendering)
"""
from __future__ import annotations

from pathlib import Path

import networkx as nx
import matplotlib
matplotlib.use("Agg")  # No display in container
import matplotlib.pyplot as plt

from spark_jobs.spark_session import get_spark


def build_graph(start_table: str, table_lineage_df, max_depth: int = 2) -> nx.DiGraph:
    """Build a DiGraph of all tables within max_depth hops of start_table."""
    edges = table_lineage_df.select("downstream_table", "upstream_table").collect()

    upstream_of: dict[str, set[str]] = {}
    downstream_of: dict[str, set[str]] = {}
    for row in edges:
        upstream_of.setdefault(row.downstream_table, set()).add(row.upstream_table)
        downstream_of.setdefault(row.upstream_table, set()).add(row.downstream_table)

    # BFS in both directions, collecting tables.
    visited: set[str] = {start_table}
    frontier = {start_table}
    for _ in range(max_depth):
        next_frontier: set[str] = set()
        for node in frontier:
            for n in upstream_of.get(node, set()) | downstream_of.get(node, set()):
                if n not in visited:
                    visited.add(n)
                    next_frontier.add(n)
        frontier = next_frontier

    # Build the graph: include only edges where BOTH endpoints are in visited.
    g = nx.DiGraph()
    for row in edges:
        if row.upstream_table in visited and row.downstream_table in visited:
            g.add_edge(row.upstream_table, row.downstream_table)
    g.add_node(start_table)  # ensure it's in the graph even if isolated
    return g


def render(g: nx.DiGraph, start_table: str, out_png: Path) -> None:
    plt.figure(figsize=(16, 10))
    pos = nx.spring_layout(g, k=2.5, iterations=80, seed=42)

    node_colors = ["#ff6961" if n == start_table else "#aec6cf" for n in g.nodes]
    nx.draw_networkx_nodes(g, pos, node_color=node_colors, node_size=1500)
    nx.draw_networkx_labels(g, pos, font_size=7)
    nx.draw_networkx_edges(g, pos, arrowsize=15, edge_color="#777", width=0.8)

    plt.title(f"Lineage around: {start_table}")
    plt.axis("off")
    plt.tight_layout()
    plt.savefig(out_png, dpi=120, bbox_inches="tight")
    print(f"Wrote {out_png}")


def main() -> None:
    spark = get_spark("visualize-lineage")
    table_lineage = spark.read.parquet("/app/data/parquet/table_lineage")

    start = "action_after_ticket_closure_base"
    g = build_graph(start, table_lineage, max_depth=2)
    print(f"Graph: {g.number_of_nodes()} nodes, {g.number_of_edges()} edges")

    out_png = Path("/app/data/lineage_graph.png")
    render(g, start, out_png)

    # Also write a DOT file for external rendering.
    out_dot = Path("/app/data/lineage_graph.dot")
    nx.drawing.nx_pydot.write_dot(g, str(out_dot))  # requires pydot, optional
    print(f"Wrote {out_dot}")

    spark.stop()


if __name__ == "__main__":
    main()