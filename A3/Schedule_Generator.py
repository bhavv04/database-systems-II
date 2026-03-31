from math import factorial
from collections import defaultdict
import re

# Part 1b - Schedule Generator
T1 = [('T1','R','X'), ('T1','W','X'), ('T1','R','Y'), ('T1','W','Y')]
T2 = [('T2','R','Z'), ('T2','R','Y'), ('T2','W','Y'),
      ('T2','R','X'), ('T2','W','X')]
T3 = [('T3','R','Y'), ('T3','R','Z'), ('T3','W','Y'), ('T3','W','Z')]

def generate_schedules(txns):
    if all(len(t) == 0 for t in txns):
        return [[]]
    schedules = []
    for i, txn in enumerate(txns):
        if not txn:
            continue
        op = txn[0]
        rest = [t if j != i else t[1:] for j, t in enumerate(txns)]
        for suffix in generate_schedules(rest):
            schedules.append([op] + suffix)
    return schedules

# Part 1c - Conflict-Serializability Checker
class ConflictSerializabilityChecker:
    def __init__(self, schedule):
        self.schedule = schedule
        self.transactions = list(dict.fromkeys(op[0] for op in schedule))
        self.graph = defaultdict(set)
        self.conflicts = []

    def find_conflicts(self):
        for i in range(len(self.schedule)):
            for j in range(i + 1, len(self.schedule)):
                ti, oi, xi = self.schedule[i]
                tj, oj, xj = self.schedule[j]
                if ti == tj or xi != xj:
                    continue
                if oi == 'R' and oj == 'R':
                    continue
                self.conflicts.append((ti, tj, oi, oj, xi))
                self.graph[ti].add(tj)
        return self.conflicts

    def has_cycle(self):
        visited, rec = set(), set()
        path_holder = [None]

        def dfs(node, path):
            visited.add(node); rec.add(node)
            for nb in self.graph.get(node, []):
                if nb not in visited:
                    if dfs(nb, path + [nb]):
                        return True
                elif nb in rec:
                    path_holder[0] = path + [nb]
                    return True
            rec.discard(node)
            return False

        for tx in self.transactions:
            if tx not in visited and dfs(tx, [tx]):
                return True, path_holder[0]
        return False, None

    def topological_sorts(self):

        # Initialise every transaction to in-degree 0 first
        in_deg = {tx: 0 for tx in self.transactions}
        for u in self.graph:
            for v in self.graph[u]:
                in_deg[v] += 1
        results = []

        def backtrack(order, deg):
            if len(order) == len(self.transactions):
                results.append(list(order)); return
            for tx in self.transactions:
                if deg[tx] == 0 and tx not in order:
                    for nb in self.graph.get(tx, []):
                        deg[nb] -= 1
                    order.append(tx)
                    backtrack(order, deg)
                    order.pop()
                    for nb in self.graph.get(tx, []):
                        deg[nb] += 1

        backtrack([], dict(in_deg))
        return results

    def check(self, label=""):
        self.find_conflicts()
        cycle, path = self.has_cycle()
        header = f"{label}" if label else "Schedule"
        print(header)
        if cycle:
            print(f"  NOT Conflict-Serializable  |  Cycle: {' -> '.join(path)}")
        else:
            orders = self.topological_sorts()
            print(f"  CONFLICT-SERIALIZABLE")
            print(f"  Equivalent serial schedules: "
                  + ", ".join(" -> ".join(o) for o in orders))
        print()
        return not cycle

def parse_schedule(s):
    s = s.replace(' ', '')
    schedule = []
    for tok in s.split(','):
        if tok.startswith('C'):
            continue
        m = re.match(r'([RW])(\d+)\((\w+)\)', tok)
        if m:
            schedule.append((f"T{m.group(2)}", m.group(1), m.group(3)))
    return schedule

# Part 1d - Run on Part 2 Schedules
if __name__ == "__main__":
    N = 4 + 5 + 4
    total = factorial(N) // (factorial(4) * factorial(5) * factorial(4))
    print(f"Total possible schedules for T1/T2/T3: {total}\n")

    s1 = "R1(X),W1(X),R3(X),W3(X),W2(X),R1(Y),W1(Y),C1,W3(Y),C3,R2(Y),W2(Y),C2"
    s2 = "R2(X),W2(X),R3(Y),W3(Y),R3(Z),W3(Z),C3,R2(Z),W2(Z),C2,R1(X),W1(X),C1"
    s3 = "W1(A),W2(B),W3(C),R1(X),R2(X),R1(Y),W1(X),C1,W2(Y),C2,W3(Y),C3"

    for label, raw in [("Schedule 1", s1),
                       ("Schedule 2", s2),
                       ("Schedule 3", s3)]:
        checker = ConflictSerializabilityChecker(parse_schedule(raw))
        checker.check(label)