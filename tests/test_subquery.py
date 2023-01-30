#%%
from sqllineage.runner import LineageRunner
from sqllineage.core.models import Column, Path, SubQuery, Table
import matplotlib.pyplot as plt
import networkx as nx
from sqllineage.utils.constant import EdgeType
from collections import defaultdict

def add_column_lineage(graph,src: Column, tgt: Column) -> None:
        graph.add_edge(src, tgt, type=EdgeType.LINEAGE)
        graph.add_edge(tgt.parent, tgt, type=EdgeType.HAS_COLUMN)
        if src.parent is not None:
            # starting NetworkX v2.6, None is not allowed as node, see https://github.com/networkx/networkx/pull/4892
            graph.add_edge(src.parent, src, type=EdgeType.HAS_COLUMN)

def test_subquery():
    TEST_QUERY = ' INSERT INTO cdw.final select A.name,A.age from (select * from (select details.age, concat(person.first_name,details.last_name) as name from staging.person join staging.details on person.id = details.id)) A'
    


    runner = LineageRunner(TEST_QUERY,encoding='utf-8',verbose= True)
    #runner.print_column_lineage()
    
    # actual = {(lineage[0], lineage[-1]) for lineage in set(runner.get_column_lineage(exclude_subquery=False))}

    runner.print_table_lineage()

    graph = runner._sql_holder.graph
    #nx.write_graphml(graph, "test.graphml")

    column_nodes = [n for n in graph.nodes if isinstance(n, Column)]
    column_graph = graph.subgraph(column_nodes)
    # mapp = defaultdict(list)
    nx.draw(column_graph,with_labels = True)
    plt.show()
    # print(column_graph)


    for path in runner.get_column_lineage(exclude_subquery=False):
            print(" <- ".join(str(col) for col in reversed(path)))
            
    #assert(True),f"\n\tExpected Lineage: {expected}\n\tActual Lineage: {actual}"

test_subquery()





# %%
