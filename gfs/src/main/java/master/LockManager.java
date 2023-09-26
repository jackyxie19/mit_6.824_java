package master;

/**
 * 管理文件的访问控制信息，如对文件的读写请求需要获取上级目录的读锁。单个文件的读锁和写锁冲突。
 */
public interface LockManager {
    /**
     * 获取指定路径的读锁
     * @param path
     * @return
     */
    int acquireReadLock(String path);

    /**
     * 获取指定路径写锁
     * @param path
     * @return
     */
    int acquireWriteLock(String path);

    /**
     * 释放读锁
     * @param path
     * @return
     */
    int releaseReadLock(String path);

    /**
     * 释放写锁
     * @param path
     * @return
     */
    int releaseWriteLock(String path);
}
