package edgelab.retryFreeDB.repo.concurrencyControl;

import edgelab.retryFreeDB.repo.storage.DTO.DBData;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class IC3Piece {



    @Getter
    private final AtomicBoolean started = new AtomicBoolean(false);

    public void start() {
        started.set(true);
        finished.set(false);
    }

    public boolean isStarted() {
        return started.get();
    }

    @Getter
    private final AtomicBoolean OCCValidated = new AtomicBoolean(false);

    public void OCCValidate() {
        OCCValidated.set(true);
    }

    public boolean isOCCValidated() {
        return OCCValidated.get();
    }


    @Getter
    private final AtomicBoolean finished = new AtomicBoolean(false);

    public void finish() {
        finished.set(true);
    }

    public boolean isFinished() {
        return finished.get();
    }


    @Getter
    @Setter
    private final String pieceName;

    @Getter
    private final Set<String> conflicts;

    private final AtomicBoolean committed = new AtomicBoolean(false);


    @Getter
    private final Set<String> readSet;
    @Getter
    private final Map<String, DBData> writeSet;

    public void commit() {
        committed.set(true);
    }

    public boolean isCommitted() {
        return committed.get();
    }

        @Getter
        private final AtomicBoolean aborted = new AtomicBoolean(false);

        public void abort() {
            aborted.set(true);
        }

        public boolean isAborted() {
            return aborted.get();
        }



    public IC3Piece(Set<String> conflicts, String name) {
        this.readSet = ConcurrentHashMap.newKeySet();
        this.writeSet = new ConcurrentHashMap<>();

        this.conflicts = ConcurrentHashMap.newKeySet();
        this.conflicts.addAll(conflicts);

        this.pieceName = name;

    }

    private List<String> toLock = null;

    public List<String> getToLock() {
        if (this.toLock == null) {
            Set<String> toLockSet = new HashSet<>(this.getReadSet());
            toLockSet.addAll(this.getWriteSet().keySet());
            this.toLock = new ArrayList<>(toLockSet);
            this.toLock.sort(String::compareTo);
        }
        return this.toLock;
    }

    public void addRead(String key) {
        this.readSet.add(key);
    }


    public void addWrite(String key, DBData value) {
        this.readSet.add(key);
        this.writeSet.put(key, value);
    }


}
