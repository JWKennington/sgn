from sgn.base import SinkPad, SourcePad


def _id(name):
    """format string for use as node ID"""
    return name.replace(":", "__")


def _text_width(names):
    """determine maximum width in points of a set of strings"""
    width = 0
    for name in names:
        width = max(width, len(name))
    width *= 9
    return width


def _color(node):
    """return node html color based on type and linkage"""
    if not node.is_linked:
        return "tomato"
    elif isinstance(node, SinkPad):
        return "lightblue"
    elif isinstance(node, SourcePad):
        return "MediumAquaMarine"


# def _element_struct_record(element):
#     """render element as "record" structure"""
#     columns = []
#     ss = []
#     for snk in element.sink_pads:
#         snk_id = _id(snk.name)
#         snk_name = element.rsnks[snk]
#         ss.append(f"<{snk_id}> {snk_name}")
#     if ss:
#         columns.append("{" + "|".join(ss) + "}")
#     columns.append("")
#     ss = []
#     for src in element.source_pads:
#         src_id = _id(src.name)
#         src_name = element.rsrcs[src]
#         ss.append(f"<{src_id}> {src_name}")
#     if ss:
#         columns.append("{" + "|".join(ss) + "}")
#     struct = element.name + "|" + "{" + "|".join(columns) + "}"
#     return struct


def _element_struct_plaintext(element):
    """render element as html structure"""
    etype = element.__class__.__name__
    # https://graphviz.org/doc/info/shapes.html#html
    struct = f"""<
<TABLE BORDER="1" CELLBORDER="0" CELLSPACING="0" CELLPADDING="0" bgcolor="DodgerBlue">
  <TR><TD COLSPAN="3" CELLPADDING="4"><b>{element.name}</b></TD></TR>
  <TR><TD COLSPAN="3" CELLPADDING="4">{etype}</TD></TR>
  <TR>
    <TD>"""
    if element.sink_pads:
        struct += """
<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">
"""
        width = _text_width(element.sink_pad_names)
        for snk in element.sink_pads:
            snk_id = _id(snk.name)
            snk_name = element.rsnks[snk]
            color = _color(snk)
            struct += f"""<TR><TD PORT="{snk_id}" fixedsize="false" width="{width}" height="30" align="left" bgcolor="{color}">{snk_name}</TD></TR>"""  # noqa E501
        struct += """
</TABLE>
"""

    struct += """
    </TD>
    <TD>-</TD>
    <TD>"""

    if element.source_pads:
        struct += """
<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">
"""
        width = _text_width(element.source_pad_names)
        for src in element.source_pads:
            src_id = _id(src.name)
            src_name = element.rsrcs[src]
            color = _color(src)
            struct += f"""<TR><TD PORT="{src_id}" fixedsize="false" width="{width}" height="30" align="right" bgcolor="{color}">{src_name}</TD></TR>"""  # noqa E501
        struct += """
</TABLE>
"""

    struct += """
    </TD>
  </TR>
</TABLE>
>"""
    return struct


def visualize(pipeline, label=None, path=None):
    """convert a pipeline to a graphviz.DiGraph object

    Source pads are light green, sink pads are light blue, and
    unconnected pads are red.

    Args:
        label: str
            label for graph
        path: Path
            If set save graph visualization to a file (format based on
            file extension).

    Returns:
        DiGraph, the graph object

    """
    try:
        import graphviz
    except ImportError:
        raise ImportError("graphviz needs to be installed to visualize pipelines")

    # create the graph
    graph = graphviz.Digraph(
        "pipeline",
        graph_attr={
            "labelloc": "t",
            "rankdir": "LR",
            "ranksep": "2",
        },
        node_attr={
            # "shape": "record",
            "shape": "plaintext",
            "fontname": "times mono",
        },
    )
    if label:
        graph.graph_attr["label"] = f"""<<font point-size="32"><b>{label}</b></font>>"""

    # create nodes for all the elemnts
    for name in pipeline.nodes(pads=False):
        element = pipeline[name]
        struct = _element_struct_plaintext(element)
        graph.node(
            element.name,
            struct,
        )

    # connect all the element pads
    for sname, tname in pipeline.edges(pads=True, intra=False):
        target = pipeline[tname]
        source = pipeline[sname]
        graph.edge(
            source.element.name + ":" + _id(source.name),
            target.element.name + ":" + _id(target.name),
        )

    if path:
        graph.render(
            outfile=path,
            cleanup=True,
        )

    return graph
