package master;

/**
 * 租约管理
 */
public interface LeaseManager<Lease,ChunkServer, Chunk> {

    /**
     * 发放租约
     * @param chunk
     * @return
     */
    ChunkServer grantLease(Chunk chunk);

    /**
     * 续约
     * @param lease
     * @return
     */
    int extentLease(Lease lease);

    int leaseExpire(Lease lease);
}
