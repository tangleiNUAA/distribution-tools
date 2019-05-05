package org.vidi.distribution.lock;

/**
 * Distributed lock for distribution system.
 *
 * @author vidi
 */
public interface DistributedLock {

    /**
     * Acquires the lock.
     * <p>
     * If the lock is not available then the current thread becomes
     * disabled for thread scheduling purposes and lies dormant until the
     * lock has been acquired.
     * <p>
     * <b>Implementation Considerations</b>
     * <p>
     * A {@code Lock} implementation may be able to detect erroneous use
     * of the lock, such as an invocation that would cause deadlock, and
     * may throw an (unchecked) exception in such circumstances.  The
     * circumstances and the exception type must be documented by that
     * {@code Lock} implementation.
     */
    void lock();

    /**
     * Releases the lock.
     * <p>
     * <b>Implementation Considerations</b>
     * <p>
     * A {@code Lock} implementation will usually impose
     * restrictions on which thread can release a lock (typically only the
     * holder of the lock can release it) and may throw
     * an (unchecked) exception if the restriction is violated.
     * Any restrictions and the exception
     * type must be documented by that {@code Lock} implementation.
     */
    void unlock();
}
