package master;

/**
 * 心跳及元数据收集
 */
public interface HeartbeatMetaCollector<Heartbeat> {

    int uploadHeartbeat(Heartbeat heartbeat);
}
