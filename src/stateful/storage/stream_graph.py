import networkx as nx


class StreamGraph:
    __root__ = "<ROOT>"

    def __init__(self, stream_names):
        self.keys = set()
        self.DAG = nx.DiGraph()
        self.DAG.add_node(self.__root__)

        for name in stream_names:
            self.add(name, [])

    def add(self, name, dependencies):
        if name not in self.keys:
            self.keys.add(name)

        if name not in self.DAG.nodes:
            self.DAG.add_node(name)

        if not dependencies:

            self.DAG.add_edge(self.__root__, name)
        else:
            for dependency in dependencies:
                assert dependency in self.keys, f"Dependency {dependency} is not known"
                if not self.DAG.has_edge(dependency, name):
                    self.DAG.add_edge(dependency, name)

    def _target_columns(self, columns):
        required_columns = set()

        target_columns = set(columns)
        while target_columns:
            column = target_columns.pop()

            required_columns.add(column)

            target_columns = (target_columns | set(self.DAG.predecessors(column))).difference(self.__root__)

        return required_columns

    def execution_order(self, columns):
        required_columns = set(self.DAG.nodes) if not columns else self._target_columns(columns)

        state = set()
        for name in self.DAG.successors(self.__root__):
            if name in required_columns and name != self.__root__:
                state.add(name)
                yield name, []

        missing_streams = [name for name in required_columns if name not in state and name != self.__root__]
        while missing_streams:
            for name in missing_streams:
                dependencies = [dependency for dependency in self.DAG.predecessors(name) if dependency != self.__root__]
                if all([dependency in state for dependency in dependencies]):
                    state.add(name)
                    yield name, dependencies

            missing_streams = [node for node in self.DAG.nodes if node not in state and node != self.__root__]

    def __contains__(self, item):
        return item in self.keys
