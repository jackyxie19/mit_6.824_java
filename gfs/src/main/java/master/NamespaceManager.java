package master;

/**
 * 管理Master内部的命名空间/目录树
 */
public interface NamespaceManager<Chunk> {
    /**
     * 创建路径
     * @param path 全路径名
     * @return 状态码
     */
    int create(String path);

    /**
     * 读取指定路径的数据
     * @param path 全路径名
     * @return 文件数据
     */
    Chunk read(String path);

    /**
     * 指定路径文件是否存在
     * @param path 全路径名
     * @return true存在，false不存在
     */
    boolean isPathExist(String path);

    /**
     * 写入文件数据，
     * TODO 考虑将写入的数据流交互放到 {@link chunkserver.Primary}中。
     * @param path 全路径
     * @param chunk 写入文件
     * @return 状态
     */
    int write(String path, Chunk chunk);
}
