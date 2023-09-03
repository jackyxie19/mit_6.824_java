package worker;

public interface FileReadWrite {
    /**
     * 将map/reduce结果写入磁盘
     */
    void write();

    /**
     * 读取map/reduce输入
     */
    void read(String id);
}
