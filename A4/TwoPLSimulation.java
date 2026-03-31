// Concurrent transactions were simulated using a round-robin scheduler. 
// A lock table was implemented using a HashMap where each record maintains its lock type (Shared or Exclusive) 
// and the transactions holding the lock. Read operations acquire shared locks while write operations acquire exclusive locks. 
// Locks are held until the transaction commits, satisfying the Two-Phase Locking protocol. 
// A log table was also implemented to record successful operations with timestamps
//  and previous log references to support potential rollback mechanisms.

import java.util.*;

class LockEntry {
    int recordId;
    String lockType; // S or X
    Set<String> transactions = new HashSet<>();

    LockEntry(int recordId, String lockType, String transaction) {
        this.recordId = recordId;
        this.lockType = lockType;
        transactions.add(transaction);
    }
}

class LogEntry {
    int timestamp;
    String type;
    String transactionId;
    int recordId;
    Integer oldValue;
    Integer newValue;
    int prevTimestamp;

    LogEntry(int ts, String type, String tid, int recordId, Integer oldV, Integer newV, int prev) {
        this.timestamp = ts;
        this.type = type;
        this.transactionId = tid;
        this.recordId = recordId;
        this.oldValue = oldV;
        this.newValue = newV;
        this.prevTimestamp = prev;
    }

    public String toString() {
        return timestamp + " | " + type + " | " + transactionId +
                " | " + recordId + " | " + oldValue + " | " + newValue +
                " | prev=" + prevTimestamp;
    }
}

class Transaction {
    String id;
    List<String> operations;
    int pointer = 0;
    boolean waiting = false;

    Transaction(String id, List<String> ops) {
        this.id = id;
        this.operations = ops;
    }

    boolean finished() {
        return pointer >= operations.size();
    }

    String nextOperation() {
        return operations.get(pointer);
    }

    void advance() {
        pointer++;
    }
}

public class TwoPLSimulation {

    static Map<Integer, Integer> database = new HashMap<>();
    static Map<Integer, LockEntry> lockTable = new HashMap<>();
    static List<LogEntry> logTable = new ArrayList<>();
    static Map<String, Integer> lastLog = new HashMap<>();

    static int timestamp = 0;

    public static void main(String[] args) {

        // initial database values
        database.put(1,1);
        database.put(2,2);
        database.put(7,7);
        database.put(9,9);

        Transaction T1 = new Transaction("T1",
                Arrays.asList("W(1,5)", "C"));

        Transaction T2 = new Transaction("T2",
                Arrays.asList("R(9)", "R(7)", "C"));

        Transaction T3 = new Transaction("T3",
                Arrays.asList("R(1)", "C"));

        List<Transaction> transactions = Arrays.asList(T1,T2,T3);

        runScheduler(transactions);
    }

    static void runScheduler(List<Transaction> transactions){

        boolean done = false;

        while(!done){

            done = true;

            for(Transaction t : transactions){

                if(t.finished()) continue;

                done = false;

                executeOperation(t);
            }
        }

        System.out.println("\nFinal Log Table");
        for(LogEntry l : logTable){
            System.out.println(l);
        }
    }

    static void executeOperation(Transaction t){

        String op = t.nextOperation();

        if(op.startsWith("R")){
            int record = Integer.parseInt(op.substring(2, op.length()-1));

            if(acquireSharedLock(t.id, record)){

                int value = database.get(record);

                int prev = lastLog.getOrDefault(t.id, -1);

                LogEntry log = new LogEntry(timestamp++,"R",t.id,record,value,null,prev);
                logTable.add(log);
                lastLog.put(t.id, log.timestamp);

                System.out.println(t.id+" reads "+value+" from "+record);

                t.advance();
            }

        } else if(op.startsWith("W")){

            String inside = op.substring(2,op.length()-1);
            String[] parts = inside.split(",");

            int record = Integer.parseInt(parts[0]);
            int value = Integer.parseInt(parts[1]);

            if(acquireExclusiveLock(t.id, record)){

                int oldValue = database.get(record);

                database.put(record,value);

                int prev = lastLog.getOrDefault(t.id,-1);

                LogEntry log = new LogEntry(timestamp++,"W",t.id,record,oldValue,value,prev);
                logTable.add(log);
                lastLog.put(t.id, log.timestamp);

                System.out.println(t.id+" writes "+value+" to "+record);

                t.advance();
            }

        } else if(op.equals("C")){

            int prev = lastLog.getOrDefault(t.id,-1);

            LogEntry log = new LogEntry(timestamp++,"C",t.id,-1,null,null,prev);
            logTable.add(log);
            lastLog.put(t.id, log.timestamp);

            releaseLocks(t.id);

            System.out.println(t.id+" commits");

            t.advance();
        }

        printLockTable();
    }

    static boolean acquireSharedLock(String tid, int record){

        if(!lockTable.containsKey(record)){
            lockTable.put(record,new LockEntry(record,"S",tid));
            return true;
        }

        LockEntry entry = lockTable.get(record);

        if(entry.lockType.equals("S")){
            entry.transactions.add(tid);
            return true;
        }

        if(entry.lockType.equals("X") && entry.transactions.contains(tid)){
            return true;
        }

        System.out.println(tid+" waiting for shared lock on "+record);
        return false;
    }

    static boolean acquireExclusiveLock(String tid, int record){

        if(!lockTable.containsKey(record)){
            lockTable.put(record,new LockEntry(record,"X",tid));
            return true;
        }

        LockEntry entry = lockTable.get(record);

        if(entry.lockType.equals("X") && entry.transactions.contains(tid)){
            return true;
        }

        if(entry.lockType.equals("S") && entry.transactions.size()==1 && entry.transactions.contains(tid)){
            entry.lockType="X";
            return true;
        }

        System.out.println(tid+" waiting for exclusive lock on "+record);
        return false;
    }

    static void releaseLocks(String tid){

        for(Integer key : new ArrayList<>(lockTable.keySet())){

            LockEntry entry = lockTable.get(key);

            entry.transactions.remove(tid);

            if(entry.transactions.isEmpty()){
                lockTable.remove(key);
            }
        }
    }

    static void printLockTable(){

        System.out.println("\nLock Table");

        if(lockTable.isEmpty()){
            System.out.println("empty");
            return;
        }

        for(LockEntry e : lockTable.values()){
            System.out.println(
                    "Record "+e.recordId+
                    " | "+e.lockType+
                    " | "+e.transactions
            );
        }

        System.out.println();
    }
}