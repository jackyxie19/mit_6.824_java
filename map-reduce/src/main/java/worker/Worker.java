package worker;

import function.MapFunction;
import lombok.Data;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import lombok.extern.slf4j.Slf4j;
import master.MapTask;
import master.Master;
import master.ReduceTask;
import master.WorkerInfo;
import tools.SnowflakeGenerator;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * 1.Map和Reduce任务队列分离，Map和Reduce任务触发的时机、执行方式、执行节点与Map任务关联性不大，考虑将两者分离。
 * 2.Map任务队列和Reduce任务队列等有读有些的共享存储需要考虑线程安全问题。
 * 3.Worker对外应该值暴露任务submit、kill、status等基础接口，真实的作业执行不应为外部感知。
 */
@Log4j2
public class Worker {
    public static final long MAP_TASK_EXECUTE_PERIOD = 1000L;
    private Master master;
    /**
     * Worker的唯一标识id
     */
    @Getter
    private final String workerId;
    /**
     * Map任务接受队列
     */
    private final List<MapTask> receivedMapTaskList = new ArrayList<>();
    /**
     * Map任务运行队列
     */
    private final List<MapTask> runningMapTaskList = new ArrayList<>();
    /**
     * Map任务完成队列
     */
    private final List<MapTask> finishedMapTaskList = new ArrayList<>();
    /**
     * Reduce任务接受队列
     */
    private final List<ReduceTask> receivedReduceTaskList = new ArrayList<>();
    /**
     * Reduce任务运行队列
     */
    private final List<ReduceTask> runningReduceTaskList = new ArrayList<>();
    /**
     * Reduce任务完成队列
     */
    private final List<ReduceTask> finishedReduceTaskList = new ArrayList<>();

    /**
     * Map任务Id和结果映射
     */
    private final Map<String, String> mapIdToMapResult = new ConcurrentHashMap<>();

    /**
     * 异步运行map任务的线程池
     */
    ExecutorService mapExecutor = Executors.newSingleThreadExecutor();
    /**
     * 异步运行reduce任务的线程池
     */
    ExecutorService reduceExecutor = Executors.newSingleThreadExecutor();

    private final Lock receivedMapTaskLock = new ReentrantLock();
    private final Lock runningMapTaskLock = new ReentrantLock();

    private final Lock finishedMapTaskLock = new ReentrantLock();

    private final Lock receivedReduceTaskLock = new ReentrantLock();

    private final Lock runningReduceTaskLock = new ReentrantLock();

    private final Lock finishedReduceTaskLock = new ReentrantLock();

    /**
     * 生成上传Master的快照信息
     *
     * @return
     */
    private WorkerInfo generateSnapshot() {

        WorkerInfo workerInfo = new WorkerInfo();
        workerInfo.setWorkerId(workerId);
        workerInfo.setGenerateMillis(System.currentTimeMillis());
        try {
            receivedMapTaskLock.lock();
            runningMapTaskLock.lock();
            finishedMapTaskLock.lock();
            receivedReduceTaskLock.lock();
            runningReduceTaskLock.lock();
            finishedReduceTaskLock.lock();

            List<MapTask> finishedMapTaskList = new ArrayList<>(this.finishedMapTaskList);
            List<MapTask> runningMapTaskList = new ArrayList<>(this.runningMapTaskList);
            List<MapTask> receivedMapTaskList = new ArrayList<>(this.receivedMapTaskList);
            List<ReduceTask> finishedReduceTaskList = new ArrayList<>(this.finishedReduceTaskList);
            List<ReduceTask> runningReduceTaskList = new ArrayList<>(this.runningReduceTaskList);
            List<ReduceTask> receivedReduceTaskList = new ArrayList<>(this.receivedReduceTaskList);
            workerInfo.setFinishedMapTaskList(finishedMapTaskList);
            workerInfo.setReceivedMapTaskList(receivedMapTaskList);
            workerInfo.setRunningMapTaskList(runningMapTaskList);
            workerInfo.setFinishedReduceTaskList(finishedReduceTaskList);
            workerInfo.setReceivedReduceTaskList(receivedReduceTaskList);
            workerInfo.setRunningReduceTaskList(runningReduceTaskList);
        } finally {
            receivedMapTaskLock.unlock();
            runningMapTaskLock.unlock();
            finishedMapTaskLock.unlock();
            receivedReduceTaskLock.unlock();
            runningReduceTaskLock.unlock();
            finishedReduceTaskLock.unlock();
        }
        return workerInfo;
    }

    private List<Future<MapTaskResult>> runningMapTask(MapTask task) {
        MapFunction mapFunction = task.getMapFunction();
        String mapTaskId = task.getTaskId();
        String inputData = task.getInputData();
        String lineSeparator = System.getProperty("line.separator");
        String[] rows = inputData.split(lineSeparator);
        List<Future<MapTaskResult>> result = new ArrayList<>();
        for (int i = 0; i < rows.length; i++) {
            String row = rows[i];
            int finalI = i;
            Future<MapTaskResult> submit = mapExecutor.submit(() -> {
                Object mapResult = mapFunction.map(row);
                MapTaskResult mapTaskResult = new MapTaskResult();
                mapTaskResult.setMapTaskId(mapTaskId);
                mapTaskResult.setResult(mapResult);
                mapTaskResult.setIndex(finalI);
                return mapTaskResult;
            });
            result.add(submit);
        }

        return result;
    }

    public Worker(Master master) {
        this();
        this.master = master;
    }

    public Worker() {
        Random random = new Random();
        int randomInt = random.nextInt();
        int low10 = ~(-1 << 10);
        randomInt = randomInt & low10;
        workerId = new SnowflakeGenerator(randomInt).generateId();
        String lineSeparator = System.getProperty("line.separator");
        // map任务定时执行
        new Thread(() -> {
            while (true) {
                long startMillis = System.currentTimeMillis();
                ArrayList<MapTask> snapshotTasks;

                try {
                    receivedMapTaskLock.lock();
                    if (receivedMapTaskList.isEmpty()){
                        // 没有Map任务直接跳过本轮提交
                        Thread.sleep(MAP_TASK_EXECUTE_PERIOD);
                        continue;
                    }
                    snapshotTasks = new ArrayList<>(receivedMapTaskList);
                    receivedMapTaskList.clear();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } finally {
                    receivedMapTaskLock.unlock();
                }

                List<List<Future<MapTaskResult>>> listFutures = snapshotTasks.stream().map(this::runningMapTask).collect(Collectors.toList());
                try {
                    // 加入运行队列
                    runningMapTaskLock.lock();
                    runningMapTaskList.addAll(snapshotTasks);
                } finally {
                    runningMapTaskLock.unlock();
                }


                listFutures.forEach(futureList -> {
                    // 存储本地结果
                    StringBuilder sb = new StringBuilder();
                    AtomicReference<String> taskId = new AtomicReference<>();
                    taskId.set("Initial Value");
                    futureList.forEach(f -> {
                        try {
                            MapTaskResult mapTaskResult = f.get(1000L, TimeUnit.MILLISECONDS);
                            sb.append(mapTaskResult.getResult().toString()).append(lineSeparator);
                            if (mapTaskResult.getMapTaskId() != null) {
                                taskId.set(mapTaskResult.mapTaskId);
                            }
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        } catch (ExecutionException e) {
                            throw new RuntimeException(e);
                        } catch (TimeoutException e) {
                            throw new RuntimeException(e);
                        }
                    });

                    mapIdToMapResult.put(taskId.get(), sb.toString());
                    log.info("map task id : {}, result : {}", taskId,sb);
                });


                // 1S 提交一次任务
                long endMillis = System.currentTimeMillis();
                long millisDuration = endMillis - startMillis;
                long sleepMillis = MAP_TASK_EXECUTE_PERIOD - millisDuration;

                try {
                    if (sleepMillis > 0) {
                        Thread.sleep(sleepMillis);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();

        // reduce任务定时执行
        new Thread(() -> {
            // 加锁获取任务列表
            // 执行任务
            // 存放结果
        }).start();

        // 定时上报信息
        new Thread(() -> {
            while (true) {
                try {
                    WorkerInfo workerInfo = generateSnapshot();
                    master.heartbeatWithTaskInfo(workerInfo);
                    Thread.sleep(1000L * 60);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();

    }


    public void submitMapTask(MapTask mapTask) {
        try {
            receivedMapTaskLock.lock();
            receivedMapTaskList.add(mapTask);
        } finally {
            receivedMapTaskLock.unlock();
        }
    }

    public WorkerInfo askForWorkerInfo(){
        return generateSnapshot();
    }

    public void submitReduceTask(ReduceTask reduceTask) {

    }

    public void killMapTask(MapTask mapTask) {

    }

    public void killReduceTask(ReduceTask reduceTask) {

    }

    public void queryMapTaskStatus(MapTask mapTask) {

    }

    public void queryReduceTaskStatus(ReduceTask reduceTask) {

    }

    @Data
    private static class MapTaskResult {
        private String mapTaskId;
        private int index;
        private Object result;
    }

}
