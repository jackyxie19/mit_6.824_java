package worker;

import worker.map.MapFunction;
import base.task.Task;
import base.task.TaskType;
import worker.map.MapTask;
import worker.reduce.ReduceTask;

import java.util.ArrayList;
import java.util.List;

/**
 * 1.Map和Reduce任务队列分离，Map和Reduce任务触发的时机、执行方式、执行节点与Map任务关联性不大，考虑将两者分离。
 * 2.Map任务队列和Reduce任务队列等有读有些的共享存储需要考虑线程安全问题。
 *
 */
public class Worker {
    private String nodeId;
    private final List<Task> tasks = new ArrayList<>();
    private final List<Task> mapTask = new ArrayList<>();
    private final List<Task> reduceTask = new ArrayList<>();


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

    private void executeMap(MapTask mapTask, MapFunction mapFunction) {
        // 执行Map任务的逻辑
//        MapExecutor<String, String> mapExecutor = new MapExecutor<>(mapper, mapTask.getInputFilePath(), yourOutputCollector);
//        mapExecutor.execute();
    }

    private void executeReduce(ReduceTask reduceTask) {
        // 执行Reduce任务的逻辑
    }
}
