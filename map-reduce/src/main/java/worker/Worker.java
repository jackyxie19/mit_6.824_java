package worker;

import function.MapFunction;
import master.MapTask;
import master.ReduceTask;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * 1.Map和Reduce任务队列分离，Map和Reduce任务触发的时机、执行方式、执行节点与Map任务关联性不大，考虑将两者分离。
 * 2.Map任务队列和Reduce任务队列等有读有些的共享存储需要考虑线程安全问题。
 * 3.Worker对外应该值暴露任务submit、kill、status等基础接口，真实的作业执行不应为外部感知。
 */
public class Worker {
    private String nodeId;
    private final List<MapTask> receivedMapTaskList = new ArrayList<>();
    private final List<MapTask> runningMapTaskList = new ArrayList<>();
    private final List<MapTask> finishedMapTaskList = new ArrayList<>();
    private final List<ReduceTask> reduceExecuteTask = new ArrayList<>();

    private Map<String, String> mapIdToMapResult = new HashMap<>();

    ExecutorService mapExecutor = Executors.newSingleThreadExecutor();
    ExecutorService reduceExecutor = Executors.newSingleThreadExecutor();
    private void heartbeatWithTaskInfo(){

    }

    private void executeMapTasks() {
        ArrayList<MapTask> snapshotTasks;
        synchronized (receivedMapTaskList){
            snapshotTasks = new ArrayList<>(receivedMapTaskList);
            receivedMapTaskList.clear();
        }
        List<Future<Object>> futures = snapshotTasks.stream().map(mapTask -> {
            return runningMapTask(mapTask);
        }).collect(Collectors.toList());

        futures.forEach(objectFuture -> {
            // 存储本地结果
            //

            mapIdToMapResult.put("id","");
        });
        runningMapTaskList.addAll(snapshotTasks);
    }


    public void submitMapTask(MapTask mapTask){
        synchronized (receivedMapTaskList){
            receivedMapTaskList.add(mapTask);
        }
    }

    private Future<Object> runningMapTask(MapTask task){
        MapFunction mapFunction = task.getMapFunction();
        Future<Object> submit = mapExecutor.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {

                return mapFunction.map();
            }
        });
        return submit;
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
