package master;

import lombok.Data;

import java.util.List;

@Data
public class WorkerInfo {
    private String workerId;
    private List<MapTask> finishedMapTaskList;
    private List<MapTask> runningMapTaskList;
    private List<MapTask> receivedMapTaskList;
    private List<ReduceTask> finishedReduceTaskList;
    private List<ReduceTask> runningReduceTaskList;
    private List<ReduceTask> receivedReduceTaskList;
    private long generateMillis;
    private long nonFinishedTaskNum;
}
