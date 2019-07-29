import pytest
from collections import OrderedDict

from pytestflow.graph import DAG, GraphError


@pytest.mark.parametrize('input_dict,expected', [
    ({'a': [1, 2, 3]}, OrderedDict(**{1: [], 2: [], 3: [], 'a': [1, 2, 3]})),
    ({'a': 'hello'}, OrderedDict(**{'hello': [], 'a': ['hello']})),
    ({'a': (1, 2, 3)}, OrderedDict(**{1: [], 2: [], 3: [], 'a': [1, 2, 3]}))
])
def test_dag_init_from_dict(input_dict, expected):
    graph = DAG(**input_dict)
    assert graph._graph == expected


def test_detect_cycle():
    input_dict = {'a': ['b', 'c'], 'b': ['d', 'a']}

    with pytest.raises(GraphError):
        DAG(**input_dict)


def test_topo_sort():
    input_dict = {'a': ['b', 'c'], 'b': ['d', 'e'], 'c': ['d'], 'd': ['e']}
    graph = DAG(**input_dict)

    assert graph.is_sorted
    assert list(graph.keys()) in (['e', 'd', 'c', 'b', 'a'], ['e', 'd', 'b', 'c', 'a'])


def test_add_edge():
    input_dict = {'a': ['b'], 'b': ['c']}
    graph = DAG(**input_dict)

    assert list(graph.keys()) == ['c', 'b', 'a']

    graph.add_edge('d', 'b')
    assert list(graph.keys()) in (['c', 'b', 'd', 'a'], ['c', 'b', 'a', 'd'])


def test_add_edge_cycle():
    input_dict = {'a': ['b'], 'b': ['c']}
    graph = DAG(**input_dict)

    assert list(graph.keys()) == ['c', 'b', 'a']

    with pytest.raises(GraphError):
        graph.add_edge('b', 'a')

    assert graph.is_sorted


def test_remove_edge():
    input_dict = {'a': ['b'], 'b': ['c'], 'd': ['b']}
    graph = DAG(**input_dict)

    assert list(graph.keys()) in (['c', 'b', 'd', 'a'], ['c', 'b', 'a', 'd'])

    graph.remove_edge('d', 'b')
    print(graph)
    assert list(graph.keys()) == ['c', 'b', 'a']
    assert 'd' not in graph
