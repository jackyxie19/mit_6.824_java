package worker;

import worker.map.Mapper;
import base.task.Task;
import base.task.TaskType;
import worker.map.MapExecutor;
import worker.map.MapTask;
import worker.reduce.ReduceTask;

import java.util.ArrayList;
import java.util.List;

public class Worker {
    private String nodeId;
    private final List<Task> tasks = new ArrayList<>();

    public void addTask(Task task) {
        tasks.add(task);
    }

        public void executeTasks() {
        for (Task task : tasks) {
            if (task.getType() == TaskType.MAP) {
                // 执行Map任务
                MapTask mapTask = (MapTask) task;
//                executeMap(mapTask);
            } else if (task.getType() == TaskType.REDUCE) {
                // 执行Reduce任务
                ReduceTask reduceTask = (ReduceTask) task;
                executeReduce(reduceTask);
            }
        }
    }

    private void executeMap(MapTask mapTask, Mapper mapper) {
        // 执行Map任务的逻辑
//        MapExecutor<String, String> mapExecutor = new MapExecutor<>(mapper, mapTask.getInputFilePath(), yourOutputCollector);
//        mapExecutor.execute();
    }

    private void executeReduce(ReduceTask reduceTask) {
        // 执行Reduce任务的逻辑
    }
}
