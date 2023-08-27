package master;

import function.ReduceFunction;
import lombok.Data;

import java.util.List;

/**
 * reduce的任务分配是根据key的hash值来判定的，如果有2个reduce任务，则一个处理hash 0 一个处理hash 1.
 */
@Data
public class ReduceTask {
    /**
     * Snowflake
     */
    private String taskId;
    private String jobId;
    private String inputLocation;
    private String resultLocation;
    /**
     * map任务的结果存放地
     */
    private List<MapTaskResultLocation> mapTaskResultLocations;
    private int status;
    private long submitMillis;
    private long receiveMillis;
    private long startMillis;
    private long endMillis;
    /**
     * reduce函数
     */
    private ReduceFunction reduceFunction;
    /**
     * 运行此reduce任务的worker
     */
    private String workerId;
    /**
     * 要处理的hash值，后续可以抽象成方法，具体处理值是什么由用户传入的partitionFunction决定。
     */
    private int designatedKeyHash;
}
