from copy import copy
from collections import OrderedDict, defaultdict
from collections.abc import MutableSequence, Callable
from typing import Optional
from functools import partial

DEFAULT_CONFIG = {'cache': True, 'cache_depth': 10, 'params': ()}


class GraphError(Exception):
    def __init__(self, graph, message):
        self.graph = graph
        self.message = message


class DAG:
    """
    Directed Acyclic Graph

    """
    def __init__(self, *args, **kwargs):
        self._graph = OrderedDict(*args, **kwargs)

        # ensure all values are mutable sequences
        for key, val in copy(self._graph).items():
            if not isinstance(val, MutableSequence):
                if isinstance(val, str):
                    self._graph[key] = [val]
                else:
                    self._graph[key] = list(val)

        for val in copy(self._graph).values():
            for v in val:
                if v not in self._graph:
                    self._graph[v] = []

        self._is_sorted = False
        self._topo_sort()

    def __getitem__(self, key):
        return self._graph[key]

    def __getattr__(self, attr):
        if attr in self._graph:
            return self._graph[attr]
        else:
            raise AttributeError(attr)

    def __iter__(self):
        return iter(self._graph)

    def __bool__(self):
        return bool(self._graph)

    @property
    def is_sorted(self) -> bool:
        return self._is_sorted

    def keys(self):
        return self._graph.keys()

    def values(self):
        return self._graph.values()

    def items(self):
        return self._graph.items()

    def add_edge(self, u, v) -> None:
        """ Add an edge to the graph """
        if u not in self._graph:
            self._graph[u] = []
        self._graph[u].append(v)

        if v not in self._graph:
            self._graph[v] = []

        try:
            self._topo_sort()
        except GraphError:
            self._is_sorted = False
            self.remove_edge(u, v)
            raise

    def remove_edge(self, u, v) -> None:
        """ Remove an edge from the graph """
        self._graph[u].remove(v)

        if not self._graph[u]:
            self._graph.pop(u)

        try:
            self._topo_sort()
        except GraphError:
            self._is_sorted = False

    def _visit(self, node, stack, visited=None) -> None:
        if visited is None:
            visited = []
        if node in stack:
            return
        elif node in visited:
            raise GraphError(self, 'Cycle detected')

        visited.append(node)
        for i in self._graph[node]:
            self._visit(i, stack, visited)

        stack[node] = self._graph[node]

    def _topo_sort(self) -> None:
        """ Topological sorting of the graph """
        stack = OrderedDict()
        for node in list(self._graph.keys()):
            self._visit(node, stack)

        # self._graph = OrderedDict(reversed(list(copy(stack).items())))
        self._graph = copy(stack)
        self._is_sorted = True

    def __repr__(self):
        return 'DAG([{}])'.format(', '.join(['({}, {})'.format(k, v)
                                             for k, v in self._graph.items()]))


class GraphBase(type):
    def __new__(cls, name, bases, attrs, **kwargs):
        super_new = super().__new__
        # Ensure initialization is only preformed only for subclasses of
        # Transform (excluding Transform itself).
        parents = [b for b in bases if isinstance(b, GraphBase)]

        if not parents:
            return super_new(cls, name, bases, attrs)

        # Create the class.
        module = attrs.pop('__module__')
        new_attrs = {'__module__': module}

        classcell = attrs.pop('__classcell__', None)

        if classcell is not None:
            new_attrs['__classcell__'] = classcell

        new_class = super_new(cls, name, bases, new_attrs)

        attr_meta = attrs.pop('Meta', None)
        meta = attr_meta or getattr(new_class, 'Meta', None)

        setattr(new_class, '_meta', DEFAULT_CONFIG.copy())

        # Next, apply any overridden values from 'class Meta'.
        if meta is not None:
            meta_attrs = meta.__dict__.copy()
            for attr_name in meta.__dict__:
                # Ignore any private attributes.
                # NOTE: We can't modify a dictionary's contents while looping
                # over it, so we loop over the *original* dictionary instead.
                if attr_name.startswith('_'):
                    del meta_attrs[attr_name]
                    continue

                new_class._meta[attr_name] = meta_attrs.pop(attr_name)

        # Add all non-callable attributes to the graph.
        new_class._graphdict = {}
        field_names = []

        attrs_copy = attrs.copy()
        for obj_name in attrs_copy:
            if (not callable(attrs[obj_name]) and not obj_name.startswith('_')
                    and not isinstance(attrs[obj_name], property)):
                new_class._graphdict[obj_name] = attrs.pop(obj_name)
            else:
                setattr(new_class, obj_name, attrs.pop(obj_name))
                field_names.append(obj_name)

        # Track fields inherited from base models.
        for base in new_class.mro():
            if base not in parents or not hasattr(base, '_meta'):
                # Things without _meta aren't functional graphs, so they're
                # uninteresting parents.
                continue

            # Add Meta fields from parent not specified in child.
            for field in base._meta:
                if field not in new_class._meta.keys():
                    new_class._meta[field] = base._meta[field]

            # Add parent graph nodes.
            for node in base._graphdict:
                if node in new_class._graphdict:
                    raise Exception(
                        'Graph node {node!r} in class {klass!r} conflicts with '
                        'node of the same name from base class {base!r}.'
                        .format(node=node, klass=name, base=base.__name__)
                    )
                elif node in new_class.__dict__:
                    raise Exception(
                        'Graph node {node!r} in class {klass!r} conflicts with '
                        'method of the same name from from base class {base!r}.'
                        .format(node=node, klass=name, base=base.__name__)
                    )
                else:
                    new_class._graphdict[node] = base._graphdict[node]

        # TODO: User-defined methods operate on the current set of graph fields
        # TODO: Register class
        new_class._prepare()

        print(name)
        print('vars:')
        print(new_class.__dict__)

        return new_class

    def _prepare(cls):
        # Give the class a docstring -- its definition.
        if cls.__doc__ is None:
            cls.__doc__ = ("{name}({fields})"
                           .format(name=cls.__name__,
                                   fields=", ".join(f for f in cls._graphdict)))


class FlowGraph(metaclass=GraphBase):
    def __init__(self, *args, **kwargs):
        self.verbose = kwargs.pop('verbose', False)
        self._graph = self._make_graph()

        # input nodes have an empty list in the adjacency list
        self._inputs = [k for k, v in self._graph.items() if not v]

        missing = set(self._inputs) - set(kwargs)

        if missing:
            raise TypeError('Missing keyword arguments: {}'.format(', '.join(missing)))

        for input in self._inputs:
            self._graphdict[input] = kwargs.get(input)

        # output nodes do not appear as inputs to any other node
        node_list = list(sum(self._graph.values(), []))
        self._outputs = [k for k in self._graph if k not in node_list]

        self._results = {}

    def __repr__(self):
        inputs = ['{}={}'.format(k, self._graphdict[k]) for k in self._inputs]
        return '{name}({inputs})'.format(name=self.__class__.__name__,
                                         inputs=', '.join(inputs))

    def __getattr__(self, attr):
        if attr in self._results:
            return self._results[attr]
        else:
            raise AttributeError(attr)

    @property
    def inputs(self):
        return self._inputs

    @property
    def outputs(self):
        return self._outputs

    def _make_graph(self) -> DAG:
        def parse_nodes(nodes):
            adj = []
            for node in nodes:
                if isinstance(node, str):
                    if '.' in node:
                        adj.append(node.split('.')[0])
                    else:
                        adj.append(node)

                elif isinstance(node, list):
                    adj += parse_nodes(node)
            return adj

        adjacency_dict = defaultdict(list)
        for key, node in self._graphdict.items():
            adjacency_dict[key] = parse_nodes(node[1:])

        return DAG(adjacency_dict)

    def execute(self):
        """ Execute the graph """
        results = {}

        def _tuple_to_func(tup):
            func = tup[0]
            args = []
            for arg in tup[1:]:
                # TODO: Account for any kind of iterable, including generators.
                if isinstance(arg, list):
                    args.append([results[x] for x in arg])
                else:
                    # TODO: Currently only works for one attribute deep. Recurse?
                    if '.' in arg:
                        var = arg.split('.')
                        args.append(getattr(results[var[0]], var[1]))
                    else:
                        args.append(results[arg])

            new_tup = tuple([func] + args)
            return partial(*new_tup)

        for key in self._graph:
            node = self._graphdict[key]
            if self.verbose:
                print('Processing node {k!r}'.format(k=key))

            if isinstance(node, tuple):
                f = _tuple_to_func(node)
                results[key] = f()
            else:
                results[key] = self._graphdict[key]
        self._results = results

        return self._results

    @classmethod
    def run(cls, *args, item: Optional[str] = None) -> Callable:
        """
        Use the graph as a node in another graph

        Parameters
        ----------
        *args
            arguments to pass to graph initializer

        item: str or list of str
            keys of the results graph to be returned

        Returns
        -------
            Function whose output is the result of the graph according to the
            keys specified
        """
        def func(*args):
            c = cls(*args)
            results = c.execute()
            if item is None:
                return results
            else:
                return results[item]
        return func
