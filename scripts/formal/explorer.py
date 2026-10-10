"""Finite labeled-state exploration with weak-action-fair lasso witnesses.

Every state gets Observe stuttering. Safety traces are shortest BFS prefixes.
Liveness searches SCCs in a pending-obligation subgraph of an environmental
suffix. A weakly fair closed walk must either disable each fair label somewhere
on the walk or execute that label on the walk. We construct and validate that
walk, rather than treating SCC membership alone as a counterexample.
"""
from collections import deque
from dataclasses import asdict, is_dataclass


class SearchLimit(RuntimeError):
    pass


def render(state):
    return asdict(state) if is_dataclass(state) else state


def explore(model, max_states=100000):
    if max_states < 1:
        raise SearchLimit("state limit must be positive")
    initial = model.initial
    states, ids, parent = [initial], {initial: 0}, [None]
    edges = []
    for source, state in enumerate(states):
        outgoing = list(model.transitions(state)) + [('Observe', state)]
        row = []
        for label, target in outgoing:
            if target not in ids:
                if len(states) >= max_states:
                    raise SearchLimit(f'{model.name}: incomplete search at {max_states} states')
                ids[target] = len(states)
                states.append(target)
                parent.append((source, label))
            dest = ids[target]
            row.append((label, dest))
            failure = model.violation(state, label, target)
            if failure:
                trace = prefix(states, parent, source)
                trace.append({'action': label, 'state': render(target)})
                return {'name': model.name, 'status': 'COUNTEREXAMPLE',
                        'complete': False, 'states': len(states),
                        'edges': sum(map(len, edges)) + len(row),
                        'bounds': model.bounds, 'properties': model.properties,
                        'assumptions': model.assumptions,
                        'property': failure, 'trace': trace}, None
        edges.append(row)
    graph = (states, edges, parent)
    report = {'name': model.name, 'status': 'PASS', 'complete': True,
              'states': len(states), 'edges': sum(map(len, edges)),
              'frontiers': sum(model.frontier(s) for s in states),
              'bounds': model.bounds, 'properties': model.properties,
              'assumptions': model.assumptions}
    for obligation in model.obligations:
        failure = fair_lasso(graph, obligation, model.suffix, model.fair_labels)
        if failure:
            report.update(status='COUNTEREXAMPLE', property='F-EVENTUAL',
                          obligation=obligation[0], **failure)
            break
    return report, graph


def prefix(states, parent, node):
    path = []
    while parent[node] is not None:
        source, action = parent[node]
        path.append({'action': action, 'state': render(states[node])})
        node = source
    path.append({'action': 'Init', 'state': render(states[node])})
    return list(reversed(path))


def components(vertices, adjacency):
    """Iterative Kosaraju; no Python recursion or depth cutoff."""
    seen, order = set(), []
    for root in sorted(vertices):
        if root in seen:
            continue
        stack = [(root, False)]
        while stack:
            node, done = stack.pop()
            if done:
                order.append(node)
            elif node not in seen:
                seen.add(node)
                stack.append((node, True))
                stack.extend((dest, False) for _, dest in reversed(adjacency[node])
                             if dest not in seen)
    reverse = {node: [] for node in vertices}
    for node in vertices:
        for _, dest in adjacency[node]:
            reverse[dest].append(node)
    seen = set()
    for root in reversed(order):
        if root in seen:
            continue
        component, stack = set(), [root]
        while stack:
            node = stack.pop()
            if node in seen:
                continue
            seen.add(node)
            component.add(node)
            stack.extend(reverse[node])
        yield component


def route(start, end, adjacency, component):
    todo, parent = deque([start]), {start: None}
    while todo:
        node = todo.popleft()
        if node == end:
            path = []
            while parent[node] is not None:
                src, action = parent[node]
                path.append((src, action, node))
                node = src
            return list(reversed(path))
        for action, dest in adjacency[node]:
            if dest in component and dest not in parent:
                parent[dest] = (node, action)
                todo.append(dest)
    raise AssertionError('SCC path missing')


def fair_lasso(graph, obligation, suffix, fair_labels):
    states, edges, parent = graph
    _, pending = obligation
    vertices = {i for i, s in enumerate(states) if pending(s) and suffix(s, None)}
    adjacency = {i: [(a, d) for a, d in edges[i]
                     if d in vertices and suffix(states[i], a)] for i in vertices}
    enabled = {i: {a for a, _ in edges[i] if suffix(states[i], a)} for i in vertices}
    for component in components(vertices, adjacency):
        cyclic = len(component) > 1 or any(d == n for n in component for _, d in adjacency[n])
        if not cyclic:
            continue
        witnesses = []
        for fair in sorted(fair_labels):
            disabled = next((n for n in sorted(component) if fair not in enabled[n]), None)
            taken = next(((n, a, d) for n in sorted(component)
                          for a, d in adjacency[n] if a == fair and d in component), None)
            if disabled is not None:
                witnesses.append((disabled, None, disabled))
            elif taken is not None:
                witnesses.append(taken)
            else:
                break
        else:
            root, walk = min(component), []
            here = root
            for src, action, dest in witnesses:
                walk += route(here, src, adjacency, component)
                if action is not None:
                    walk.append((src, action, dest))
                here = dest
            walk += route(here, root, adjacency, component)
            if not walk:
                action, dest = next((a, d) for a, d in adjacency[root] if d in component)
                walk = [(root, action, dest)] + route(dest, root, adjacency, component)
            visited = {src for src, _, _ in walk} | {d for _, _, d in walk}
            actions = {a for _, a, _ in walk}
            assert all(f in actions or any(f not in enabled[n] for n in visited)
                       for f in fair_labels), 'constructed lasso is not weakly fair'
            assert walk[-1][2] == walk[0][0]
            return {'trace': prefix(states, parent, root),
                    'cycle': [{'action': a, 'state': render(states[d])} for _, a, d in walk],
                    'fairness': sorted(fair_labels)}
    return None
