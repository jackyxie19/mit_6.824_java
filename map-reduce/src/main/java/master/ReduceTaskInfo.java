package master;

import function.ReduceFunction;

import java.util.List;

public class ReduceTaskInfo {

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
