import java.util.*;

//The program simulates Two-Phase Locking by running multiple transactions concurrently in round-robin order, where each transaction must acquire a shared lock before reading a record or an exclusive lock before writing, and if a lock can't be granted the transaction waits and retries next round. A log table records every operation with a timestamp and a prev pointer that chains each transaction's entries together, enabling rollback by scanning the log in reverse and restoring old values. For deadlock detection, a wait-for graph tracks which transactions are blocked by which others, and when a cycle is found via DFS the highest-ID transaction is chosen as the victim, its writes are rolled back using the log, its locks are released, and execution continues with the remaining transactions.

//  Data structures

class LockEntry {
    int recordId;
    String lockType;          // "S" or "X"
    Set<String> transactions = new HashSet<>();

    LockEntry(int recordId, String lockType, String transaction) {
        this.recordId = recordId;
        this.lockType = lockType;
        transactions.add(transaction);
    }
}

class LogEntry {
    int    timestamp;
    String type;          // R, W, C, A
    String transactionId;
    int    recordId;
    Integer oldValue;
    Integer newValue;
    int    prevTimestamp;

    LogEntry(int ts, String type, String tid,
             int recordId, Integer oldV, Integer newV, int prev) {
        this.timestamp    = ts;
        this.type         = type;
        this.transactionId = tid;
        this.recordId     = recordId;
        this.oldValue     = oldV;
        this.newValue     = newV;
        this.prevTimestamp = prev;
    }

    public String toString() {
        return timestamp + " | " + type + " | " + transactionId
             + " | " + recordId + " | " + oldValue
             + " | " + newValue + " | prev=" + prevTimestamp;
    }
}

class Transaction {
    String       id;
    List<String> operations;
    int          pointer   = 0;
    boolean      aborted   = false;
    boolean      committed = false;

    Transaction(String id, List<String> ops) {
        this.id         = id;
        this.operations = ops;
    }

    boolean finished()        { return aborted || committed || pointer >= operations.size(); }
    String  nextOperation()   { return operations.get(pointer); }
    void    advance()         { pointer++; }
}

//  Main simulation class

public class TwoPLDeadlockSimulation {

    // Shared state - reset between test runs via resetState()
    static Map<Integer, Integer>        database  = new HashMap<>();
    static Map<Integer, LockEntry>      lockTable = new HashMap<>();
    static List<LogEntry>               logTable  = new ArrayList<>();
    static Map<String, Integer>         lastLog   = new HashMap<>();
    static Map<String, Set<String>>     waitGraph = new HashMap<>();
    static Map<String, Transaction>     txMap     = new HashMap<>();
    static int timestamp = 0;

    // When true, executeOperation / abortTransaction suppress their
    // mid-step prints so the final summary doesn't get duplicate output.
    static boolean suppressMidPrint = false;

    //  Entry point - three separate test runs

    public static void main(String[] args) {

        // ══════════════════════════════════════
        //  TEST 1 - Part (a) required schedule
        //  T1: W(1,5); C
        //  T2: R(9); R(7); C
        //  T3: R(1); C
        // ══════════════════════════════════════
        System.out.println("  TEST 1 - Part (a): Required Schedule");

        resetState();
        database.put(1, 10);   // initial values for records used
        database.put(7, 70);
        database.put(9, 90);

        Transaction a_T1 = new Transaction("T1", Arrays.asList("W(1,5)", "C"));
        Transaction a_T2 = new Transaction("T2", Arrays.asList("R(9)", "R(7)", "C"));
        Transaction a_T3 = new Transaction("T3", Arrays.asList("R(1)", "C"));

        runScheduler(Arrays.asList(a_T1, a_T2, a_T3), /*deadlockEnabled=*/false);

        //  TEST 2 - Part (b) required schedule
        //  Sequential: T1 fully first, then T2
        //  T1:W(1,5); T1:R(2); T1:W(2,3); T1:R(1); T1:C
        //  T2:R(1); T2:W(1,2); T2:C

        System.out.println("  TEST 2 - Part (b): Required Log Test");

        resetState();
        database.put(1, 1);
        database.put(2, 2);

        // Build as a single interleaved schedule matching the spec:
        // T1:W(1,5); T1:R(2); T1:W(2,3); T1:R(1); T1:C; T2:R(1); T2:W(1,2); T2:C
        // Because T1 commits before T2 starts, there is no lock conflict.
        Transaction b_T1 = new Transaction("T1",
                Arrays.asList("W(1,5)", "R(2)", "W(2,3)", "R(1)", "C"));
        Transaction b_T2 = new Transaction("T2",
                Arrays.asList("R(1)", "W(1,2)", "C"));

        runScheduler(Arrays.asList(b_T1, b_T2), /*deadlockEnabled=*/false);

        //  TEST 3 - Bonus: Deadlock detection
        //  T1: R(1); W(2,1); C
        //  T2: R(2); R(3); W(1,2); C
        //  T3: R(1); W(3,3); C
        System.out.println("  TEST 3 - Bonus: Deadlock Detection");

        resetState();
        database.put(1, 1);
        database.put(2, 2);
        database.put(3, 3);

        Transaction c_T1 = new Transaction("T1", Arrays.asList("R(1)", "W(2,1)", "C"));
        Transaction c_T2 = new Transaction("T2", Arrays.asList("R(2)", "R(3)", "W(1,2)", "C"));
        Transaction c_T3 = new Transaction("T3", Arrays.asList("R(1)", "W(3,3)", "C"));

        runScheduler(Arrays.asList(c_T1, c_T2, c_T3), /*deadlockEnabled=*/true);
    }

    //  Reset all shared state between test runs

    static void resetState() {
        database.clear();
        lockTable.clear();
        logTable.clear();
        lastLog.clear();
        waitGraph.clear();
        txMap.clear();
        timestamp = 0;
        suppressMidPrint = false;
    }

    //  Round-robin scheduler

    static void runScheduler(List<Transaction> transactions, boolean deadlockEnabled) {

        for (Transaction t : transactions) {
            txMap.put(t.id, t);
            waitGraph.put(t.id, new HashSet<>());
        }

        boolean done = false;
        while (!done) {
            done = true;
            for (Transaction t : transactions) {
                if (t.finished()) continue;
                done = false;

                executeOperation(t);    // mid-step prints ON during scheduling

                if (deadlockEnabled) {
                    String victim = detectDeadlockVictim();
                    if (victim != null) {
                        System.out.println("\n*** DEADLOCK DETECTED ***");
                        System.out.println("Aborting victim: " + victim);
                        abortTransaction(victim);
                    }
                }
            }
        }

        // suppress duplicate sub-prints
        suppressMidPrint = true;

        System.out.println("\n===== FINAL DATABASE =====");
        printDatabase();

        System.out.println("\n===== FINAL LOG TABLE =====");
        printLogTable();

        System.out.println("\n===== FINAL LOCK TABLE =====");
        printLockTable();

        if (deadlockEnabled) {
            System.out.println("\n===== FINAL WAIT-FOR GRAPH =====");
            printWaitGraph();
        }

        suppressMidPrint = false;
    }

    //  Execute one operation for a transaction

    static void executeOperation(Transaction t) {
        if (t.finished()) return;

        String op = t.nextOperation();
        System.out.println("\n--> Executing " + t.id + ": " + op);

        if (op.startsWith("R")) {
            int record = Integer.parseInt(op.substring(2, op.length() - 1));

            if (acquireSharedLock(t.id, record)) {
                clearWaitEdges(t.id);
                int value = database.get(record);
                int prev  = lastLog.getOrDefault(t.id, -1);

                LogEntry log = new LogEntry(timestamp++, "R", t.id, record, value, null, prev);
                logTable.add(log);
                lastLog.put(t.id, log.timestamp);

                System.out.println(t.id + " reads " + value + " from record " + record);
                t.advance();

                if (!suppressMidPrint) { printLogTable(); }
            }

        } else if (op.startsWith("W")) {
            String   inside = op.substring(2, op.length() - 1);
            String[] parts  = inside.split(",");
            int record = Integer.parseInt(parts[0]);
            int value  = Integer.parseInt(parts[1]);

            if (acquireExclusiveLock(t.id, record)) {
                clearWaitEdges(t.id);
                int oldValue = database.get(record);
                database.put(record, value);
                int prev = lastLog.getOrDefault(t.id, -1);

                LogEntry log = new LogEntry(timestamp++, "W", t.id, record, oldValue, value, prev);
                logTable.add(log);
                lastLog.put(t.id, log.timestamp);

                System.out.println(t.id + " writes " + value + " to record " + record);
                t.advance();

                if (!suppressMidPrint) { printLogTable(); }
            }

        } else if (op.equals("C")) {
            clearWaitEdges(t.id);
            int prev = lastLog.getOrDefault(t.id, -1);

            LogEntry log = new LogEntry(timestamp++, "C", t.id, -1, null, null, prev);
            logTable.add(log);
            lastLog.put(t.id, log.timestamp);

            releaseLocks(t.id);
            t.committed = true;

            System.out.println(t.id + " commits");
            t.advance();

            if (!suppressMidPrint) { printLogTable(); }
        }

        if (!suppressMidPrint) {
            printLockTable();
            printWaitGraph();
        }
    }

    //  Lock acquisition

    static boolean acquireSharedLock(String tid, int record) {
        if (!lockTable.containsKey(record)) {
            lockTable.put(record, new LockEntry(record, "S", tid));
            return true;
        }

        LockEntry entry = lockTable.get(record);

        // Already holds any lock on this record
        if (entry.transactions.contains(tid)) return true;

        // Another transaction holds X lock - must wait
        if (entry.lockType.equals("X")) {
            addWaitEdges(tid, entry.transactions);
            System.out.println(tid + " waiting for shared lock on record " + record);
            return false;
        }

        // Shared lock exists - can share it
        entry.transactions.add(tid);
        return true;
    }

    static boolean acquireExclusiveLock(String tid, int record) {
        if (!lockTable.containsKey(record)) {
            lockTable.put(record, new LockEntry(record, "X", tid));
            return true;
        }

        LockEntry entry = lockTable.get(record);

        // Already holds X lock
        if (entry.lockType.equals("X") && entry.transactions.contains(tid)) return true;

        // Upgrade: sole holder of S lock → promote to X
        if (entry.lockType.equals("S")
                && entry.transactions.size() == 1
                && entry.transactions.contains(tid)) {
            entry.lockType = "X";
            return true;
        }

        // Other transactions hold a lock - compute blockers (exclude self)
        Set<String> blockers = new HashSet<>(entry.transactions);
        blockers.remove(tid);

        if (blockers.isEmpty()) {
            // Self is only holder (S lock upgrade case handled above; shouldn't reach here)
            entry.lockType = "X";
            return true;
        }

        addWaitEdges(tid, blockers);
        System.out.println(tid + " waiting for exclusive lock on record " + record);
        return false;
    }

    static void releaseLocks(String tid) {
        for (Integer key : new ArrayList<>(lockTable.keySet())) {
            LockEntry entry = lockTable.get(key);
            entry.transactions.remove(tid);
            if (entry.transactions.isEmpty()) lockTable.remove(key);
        }
        waitGraph.remove(tid);
        for (Set<String> edges : waitGraph.values()) edges.remove(tid);
    }

    //  Wait-for graph

    static void addWaitEdges(String waitingTx, Set<String> holders) {
        waitGraph.putIfAbsent(waitingTx, new HashSet<>());
        for (String holder : holders)
            if (!holder.equals(waitingTx))
                waitGraph.get(waitingTx).add(holder);
    }

    static void clearWaitEdges(String tid) {
        waitGraph.putIfAbsent(tid, new HashSet<>());
        waitGraph.get(tid).clear();
    }

    //  Deadlock detection (DFS cycle detection)

    static String detectDeadlockVictim() {
        Set<String>  visited    = new HashSet<>();
        Set<String>  stack      = new HashSet<>();
        List<String> cycleNodes = new ArrayList<>();

        for (String node : waitGraph.keySet()) {
            if (dfsCycle(node, visited, stack, cycleNodes)) {
                // Victim = transaction with highest ID (lowest priority)
                String victim = cycleNodes.get(0);
                for (String tx : cycleNodes)
                    if (tx.compareTo(victim) > 0) victim = tx;
                return victim;
            }
        }
        return null;
    }

    static boolean dfsCycle(String node, Set<String> visited,
                             Set<String> stack, List<String> cycleNodes) {
        if (stack.contains(node))    { cycleNodes.add(node); return true; }
        if (visited.contains(node))  return false;

        visited.add(node);
        stack.add(node);

        for (String neighbor : waitGraph.getOrDefault(node, Collections.emptySet())) {
            if (dfsCycle(neighbor, visited, stack, cycleNodes)) {
                cycleNodes.add(node);
                return true;
            }
        }
        stack.remove(node);
        return false;
    }

    //  Abort and rollback

    static void abortTransaction(String tid) {
        Transaction victim = txMap.get(tid);
        if (victim == null || victim.aborted || victim.committed) return;

        // 1. Log the abort entry first (spec step 7)
        int prev = lastLog.getOrDefault(tid, -1);
        LogEntry abortLog = new LogEntry(timestamp++, "A", tid, -1, null, null, prev);
        logTable.add(abortLog);
        lastLog.put(tid, abortLog.timestamp);
        System.out.println("Abort log added for " + tid);

        // 2. Roll back writes in reverse order using the log
        rollbackTransaction(tid);

        // 3. Release all locks
        releaseLocks(tid);

        // 4. Mark as aborted
        victim.aborted = true;
        System.out.println(tid + " has been aborted and rolled back.");

        if (!suppressMidPrint) {
            printDatabase();
            printLogTable();
            printLockTable();
            printWaitGraph();
        }
    }

    static void rollbackTransaction(String tid) {
        System.out.println("Rolling back writes for " + tid + "...");
        for (int i = logTable.size() - 1; i >= 0; i--) {
            LogEntry log = logTable.get(i);
            if (log.transactionId.equals(tid) && log.type.equals("W")) {
                database.put(log.recordId, log.oldValue);
                System.out.println("Rollback: restored record " + log.recordId
                        + " from " + log.newValue + " back to " + log.oldValue);
            }
        }
    }

    //  Print helpers

    static void printLockTable() {
        System.out.println("\nLock Table");
        if (lockTable.isEmpty()) { System.out.println("  (empty)"); return; }
        for (LockEntry e : lockTable.values())
            System.out.println("  Record " + e.recordId + " | " + e.lockType + " | " + e.transactions);
    }

    static void printLogTable() {
        System.out.println("\nLog Table");
        if (logTable.isEmpty()) { System.out.println("  (empty)"); return; }
        for (LogEntry l : logTable) System.out.println("  " + l);
    }

    static void printWaitGraph() {
        System.out.println("\nWait-For Graph");
        boolean empty = true;
        for (String tx : waitGraph.keySet()) {
            Set<String> edges = waitGraph.get(tx);
            if (!edges.isEmpty()) {
                empty = false;
                System.out.println("  " + tx + " -> " + edges);
            }
        }
        if (empty) System.out.println("  (empty)");
    }

    static void printDatabase() {
        System.out.println("Database State:");
        List<Integer> keys = new ArrayList<>(database.keySet());
        Collections.sort(keys);
        for (Integer k : keys)
            System.out.println("  Record " + k + " = " + database.get(k));
    }
}