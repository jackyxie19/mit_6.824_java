package worker;

import master.MapTask;
import master.ReduceTask;
import worker.map.MapFunction;
import worker.map.MapExecuteTask;
import worker.reduce.ReduceExecuteTask;

import java.util.ArrayList;
import java.util.List;

/**
 * 1.Map和Reduce任务队列分离，Map和Reduce任务触发的时机、执行方式、执行节点与Map任务关联性不大，考虑将两者分离。
 * 2.Map任务队列和Reduce任务队列等有读有些的共享存储需要考虑线程安全问题。
 * 3.Worker对外应该值暴露任务submit、kill、status等基础接口，真实的作业执行不应为外部感知。
 */
public class Worker {
    private String nodeId;
    private final List<MapExecuteTask> mapExecuteTask = new ArrayList<>();
    private final List<ReduceExecuteTask> reduceExecuteTask = new ArrayList<>();


    private void heartbeatWithTaskInfo(){

    }

    private void executeMapTasks() {

    }

    private void executeMap(MapExecuteTask mapExecuteTask, MapFunction mapFunction) {
        // 执行Map任务的逻辑
//        MapExecutor<String, String> mapExecutor = new MapExecutor<>(mapper, mapTask.getInputFilePath(), yourOutputCollector);
//        mapExecutor.execute();
    }

    private void executeReduce(ReduceExecuteTask reduceExecuteTask) {
        // 执行Reduce任务的逻辑
    }

    public void submitMapTask(MapTask mapTask){

    }

    public void submitReduceTask(ReduceTask reduceTask){

    }

    public void killMapTask(MapTask mapTask){

    }

    public void killReduceTask(ReduceTask reduceTask){

    }

    public void queryMapTaskStatus(MapTask mapTask){

    }

    public void queryReduceTaskStatus(ReduceTask reduceTask){

    }
}
