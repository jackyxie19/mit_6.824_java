package master;

/**
 * 块服务器节点管理
 */
public interface ChunkServerManager<ChunkServer> {
    /**
     * 注册节点
     * @param chunkServer
     * @return
     */
    int register(ChunkServer chunkServer);

    /**
     * 取消注册
     * @param chunkServer
     * @return
     */
    int deregister(ChunkServer chunkServer);

    /**
     * 判断是否注册
     * @param chunkServer
     * @return
     */
    boolean isRegistered(ChunkServer chunkServer);
}
