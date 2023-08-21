package master;

import lombok.Data;

import java.util.List;

@Data
public class WorkerInfo {
    private String workerId;
    private List<MapTask> finishedMapTasks;
    private List<MapTask> runningMapTasks;
    private List<MapTask> scheduledMapTasks;
    private List<ReduceTask> finishedReduceTasks;
    private List<ReduceTask> runningReduceTasks;
    private List<ReduceTask> scheduledReduceTasks;
}
