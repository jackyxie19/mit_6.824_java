package chunkserver;

/**
 * 心跳及快照制作
 */
public interface HeartbeatAndMetaBuilder<Heartbeat> {
    Heartbeat build();
}
