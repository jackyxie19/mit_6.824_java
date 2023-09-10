package worker;

public interface FileReadWrite {
    /**
     * 将map/reduce结果写入磁盘
     */
    void write(String path, String data);

    /**
     * 读取map/reduce输入
     */
    String read(String path);
}
