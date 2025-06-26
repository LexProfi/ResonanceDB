package ai.evacortex.resonancedb.core.storage.util;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

public final class AutoLock implements AutoCloseable {
    private final Lock lock;

    private AutoLock(Lock lock) {
        this.lock = lock;
        this.lock.lock();
    }

    public static AutoLock read(ReadWriteLock rw) {
        return new AutoLock(rw.readLock());
    }

    public static AutoLock write(ReadWriteLock rw) {
        return new AutoLock(rw.writeLock());
    }

    @Override
    public void close() {
        lock.unlock();
    }
}