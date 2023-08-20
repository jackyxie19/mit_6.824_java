package master;

import worker.map.MapTask;
import worker.reduce.ReduceTask;
import worker.Worker;

import java.util.ArrayList;
import java.util.List;

public class JobManager {
    private final List<Worker> workers = new ArrayList<>();

    public void submitJob(List<MapTask> mapTasks, List<ReduceTask> reduceTasks) {
        // 根据Worker节点的资源情况，将任务分配给不同的Worker
        distributeTasks(mapTasks, reduceTasks);

        // 执行任务
        executeTasks();
    }

    private void distributeTasks(List<MapTask> mapTasks, List<ReduceTask> reduceTasks) {
        // 实现任务分配逻辑
    }

    private void executeTasks() {
        for (Worker worker : workers) {
            worker.executeTasks();
        }
    }
}