import pytest

from pytestflow.graph import FlowGraph


def test_subclass():
    class Foo(FlowGraph):
        one = ('one', 'a', 'b')
        two = ('two', 'one', 'c')

    with pytest.raises(TypeError):
        Foo()

    instance = Foo(a=1, b=2, c=3)

    assert instance.outputs == ['two']
    assert instance.inputs == ['a', 'b', 'c']

    assert list(instance._graph._graph.keys()) in (['a', 'b', 'one', 'c', 'two'],
                                                   ['b', 'a', 'one', 'c', 'two'])
