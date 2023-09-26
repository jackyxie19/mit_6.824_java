package master;

/**
 * 操作日志管理
 */
public interface OperationLogManager<OperationLog> {
    /**
     * 写入操作日志
     * @param log
     * @return
     */
    int writeLog(OperationLog log);

    /**
     * 读取至上一检查点的log
     * @return
     */
    OperationLog readLog();

    /**
     * 回放日志操作
     * @param log
     * @return
     */
    int replayOperationLog(OperationLog log);
}
