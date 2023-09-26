package master;

/**
 * 检查点的制作与恢复
 */
public interface CheckpointManager<Snapshot> {
    /**
     * 制作检查点快照
     */
    void checkpoint();

    /**
     * 获取最近一个检查点快照
     * @return
     */
    Snapshot getLastCheckpoint();
}
