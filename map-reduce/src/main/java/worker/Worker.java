package worker;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import exception.MissingMapResultException;
import function.MapFunction;
import function.ReduceFunction;
import lombok.Data;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import master.*;
import master.enums.TaskStatus;
import tools.SnowflakeGenerator;

import java.util.*;
import java.util.concurrent.*;
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
    public static final long REDUCE_TASK_EXECUTE_PERIOD = 1000L;
    private final Map<String, Worker> workerIdMap = new HashMap<>();

    private MapResultManager mapResultManager;
    private ReduceResultManager reduceResultManager;
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
     * Reduce任务Id和结果映射
     */
    private final Map<String, String> reduceIdToReduceResult = new ConcurrentHashMap<>();

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

    /**
     * 将Map任务按行为单位提交到线程池运算
     *
     * @param task
     * @return
     */
    private Future<MapTaskResult> runningMapTask(MapTask task) {

        return mapExecutor.submit(() -> {
            try {
                MapTaskResult mapTaskResult = new MapTaskResult();
                MapFunction mapFunction = task.getMapFunction();
                String mapTaskId = task.getTaskId();
                mapTaskResult.setMapTaskId(mapTaskId);
                String inputData = task.getInputData();
                String mapResult = mapFunction.map(inputData);
                mapTaskResult.setResult(mapResult);
                mapTaskResult.setMapTask(task);

                // store to local file system
                mapResultManager.write(mapTaskId, mapTaskResult.getResult());
                mapTaskResult.getMapTask().setStatus(TaskStatus.FINISHED.getCode());
                log.info("map task id : {}, result : {}", mapTaskId, mapTaskResult.getResult());

                return mapTaskResult;
            } catch (Exception e) {
                // todo 异常时通知上报master
                return null;
            }
        });

    }

    /**
     * 将Map任务按行为单位提交到线程池运算
     *
     * @param task
     * @return
     */
    private Future<ReduceTaskResult> runningReduceTask(ReduceTask task) {

        return mapExecutor.submit(() -> {
            try {
                ReduceTaskResult reduceTaskResult = new ReduceTaskResult();

                ReduceFunction reduceFunction = task.getReduceFunction();
                String reduceTaskId = task.getTaskId();
                List<MapTaskResultLocation> mapTaskResultLocations = task.getMapTaskResultLocations();
                Map<String, List<String>> mapKeyValues = new HashMap<>();
                mapTaskResultLocations.forEach(mapTaskResultLocation -> {
                    // 读取单个map任务结果
                    String result = loadMapTaskResult(mapTaskResultLocation);
                    JSONObject mapResultJson = JSON.parseObject(result);
                    Set<String> mapKeys = mapResultJson.keySet();

                    for (String key : mapKeys) {
                        // 根据hash收集此reduce任务要处理的key集合
                        if (key.hashCode() % 4 == task.getDesignatedKeyHash()) {
                            // 本任务处理的key
                            String value = mapResultJson.getString(key);
                            if (!mapKeyValues.containsKey(key)) {
                                mapKeyValues.put(key, new ArrayList<>());
                            }
                            List<String> values = mapKeyValues.get(key);
                            values.add(value);
                        }
                    }
                });
                // reducing
                Map<String, String> currentReduceTaskResult = new HashMap<>();
                for (Map.Entry<String, List<String>> entry : mapKeyValues.entrySet()) {
                    String key = entry.getKey();
                    List<String> mapValues = entry.getValue();
                    String aggregation = mapValues.get(0);
                    for (int i = 1; i < mapValues.size(); i++) {
                        String currentValue = mapValues.get(i);
                        if (currentValue == null || currentValue.isEmpty()) continue;
                        aggregation = reduceFunction.reduce(key, aggregation, currentValue);
                    }
                    // 输出单个key最终结果
                    currentReduceTaskResult.put(key, aggregation);
                }

                // store to local, using memory instead of local disk
                reduceIdToReduceResult.put(reduceTaskId, JSON.toJSONString(currentReduceTaskResult));

                task.setStatus(TaskStatus.FINISHED.getCode());
                log.info("reduce task id : {}, result : {}", reduceTaskId, reduceIdToReduceResult);

                return reduceTaskResult;
            } catch (Exception e) {
                // todo 异常时通知上报master
                return null;
            }
        });

    }

    TaskManager taskManager;

    private void heartbeatWithTaskInfo() {
        HeartbeatMsg heartbeatMsg = new HeartbeatMsg();
        heartbeatMsg.setNodeId(workerId);
        List<String> completedMapTasks = new ArrayList<>(mapIdToMapResult.keySet());
        heartbeatMsg.setCompletedMapTasks(completedMapTasks);
        taskManager.receiveHeartbeat(heartbeatMsg);
    }


    public Worker(Master master) {
        this();
        this.master = master;
    }

    public Worker() {
        mapResultManager = new MapResultManager(this);
        reduceResultManager = new ReduceResultManager(this);
        Random random = new Random();
        int randomInt = random.nextInt();
        int low10 = ~(-1 << 10);
        randomInt = randomInt & low10;
        workerId = new SnowflakeGenerator(randomInt).generateIdWithSpin();

        // map任务定时执行
        new Thread(() -> {
            while (true) {
                try {
                    long startMillis = System.currentTimeMillis();
                    ArrayList<MapTask> snapshotTasks;

                    try {
                        receivedMapTaskLock.lock();
                        if (receivedMapTaskList.isEmpty()) {
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

                    try {
                        // 加入运行队列
                        runningMapTaskLock.lock();
                        snapshotTasks.forEach(e -> e.setStatus(TaskStatus.RUNNING.getCode()));
                        runningMapTaskList.addAll(snapshotTasks);
                    } finally {
                        runningMapTaskLock.unlock();
                    }

                    snapshotTasks.forEach(this::runningMapTask);


                    List<MapTask> finishedMapTask = snapshotTasks.stream().filter(e -> e.getStatus() == TaskStatus.FINISHED.getCode()).collect(Collectors.toList());
                    finishedMapTaskLock.lock();
                    finishedMapTaskList.addAll(finishedMapTask);
                    finishedMapTaskLock.unlock();
                    runningMapTaskLock.lock();
                    runningMapTaskList.removeAll(finishedMapTask);
                    runningMapTaskLock.unlock();

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
                } catch (Exception e) {
                    log.error("提交map任务时遇到异常:", e);
                }

            }
        }).start();

        // reduce任务定时执行
        new Thread(() -> {
            while (true) {
                try {
                    long startMillis = System.currentTimeMillis();
                    ArrayList<ReduceTask> snapshotTasks;

                    try {
                        receivedReduceTaskLock.lock();
                        if (receivedReduceTaskList.isEmpty()) {
                            // 没有Map任务直接跳过本轮提交
                            Thread.sleep(REDUCE_TASK_EXECUTE_PERIOD);
                            continue;
                        }
                        snapshotTasks = new ArrayList<>(receivedReduceTaskList);
                        receivedMapTaskList.clear();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } finally {
                        receivedReduceTaskLock.unlock();
                    }

                    try {
                        // 加入运行队列
                        runningReduceTaskLock.lock();
                        snapshotTasks.forEach(e -> e.setStatus(TaskStatus.RUNNING.getCode()));
                        runningReduceTaskList.addAll(snapshotTasks);
                    } finally {
                        runningReduceTaskLock.unlock();
                    }

                    snapshotTasks.forEach(this::runningReduceTask);

                    // TODO
                    List<ReduceTask> finishedReduceTask = snapshotTasks.stream().filter(e -> e.getStatus() == TaskStatus.FINISHED.getCode()).collect(Collectors.toList());
                    finishedReduceTaskLock.lock();
                    finishedReduceTaskList.addAll(finishedReduceTask);
                    finishedReduceTaskLock.unlock();
                    runningReduceTaskLock.lock();
                    runningReduceTaskList.removeAll(finishedReduceTask);
                    runningReduceTaskLock.unlock();

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
                } catch (Exception e) {
                    log.error("提交reduce任务时遇到异常：", e);
                }
            }
            // 加锁获取任务列表
            // 执行任务
            // 存放结果
        }).start();

        // 定时上报信息
        new Thread(() -> {
            while (true) {
                try {
                    log.info("upload worker info ");
                    WorkerInfo workerInfo = generateSnapshot();
                    master.heartbeatWithTaskInfo(workerInfo);
                    Thread.sleep(1000L * 60);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();

        // MapTask定时清理running、finished、result队列，避免内存溢出。
        new Thread(() -> {
            runningMapTaskLock.lock();
            List<MapTask> finishedTask = runningMapTaskList.stream().filter(e -> e.getStatus() == TaskStatus.FINISHED.getCode()).collect(Collectors.toList());
            runningMapTaskList.removeAll(finishedTask);
            runningMapTaskLock.unlock();
            finishedMapTaskLock.lock();
            finishedMapTaskList.addAll(finishedTask);
            finishedMapTaskLock.unlock();
            // TODO 感知长时间未完成的任务，并做后续的处理

        }).start();

        // ReduceTask定时清理running、finished、result队列，避免内存溢出。
        new Thread(() -> {
            runningReduceTaskLock.lock();
            List<ReduceTask> finishedTask = runningReduceTaskList.stream().filter(e -> e.getStatus() == TaskStatus.FINISHED.getCode()).collect(Collectors.toList());
            runningReduceTaskList.removeAll(finishedTask);
            runningReduceTaskLock.unlock();
            finishedReduceTaskLock.lock();
            finishedReduceTaskList.addAll(finishedTask);
            finishedReduceTaskLock.unlock();
            // TODO 感知长时间未完成的任务，并做后续的处理

        }).start();
    }

    public void postRegistered(Map<String, Worker> workerMap) {
        workerIdMap.putAll(workerMap);
    }

    public void submitMapTask(MapTask mapTask) {
        try {
            receivedMapTaskLock.lock();
            mapTask.setStatus(TaskStatus.SUBMITTED.getCode());
            receivedMapTaskList.add(mapTask);
        } finally {
            receivedMapTaskLock.unlock();
        }
    }

    public WorkerInfo askForWorkerInfo() {
        return generateSnapshot();
    }

    /**
     * TM提交Reduce任务
     *
     * @param reduceTask
     */
    public void submitReduceTask(ReduceTask reduceTask) {
        try {
            receivedReduceTaskLock.lock();
            receivedReduceTaskList.add(reduceTask);
        } finally {
            receivedReduceTaskLock.unlock();
        }
    }

    public String loadMapTaskResult(MapTaskResultLocation mapTaskResultLocation) {
        String mapTaskId = mapTaskResultLocation.getMapTaskId();
        String mapTaskResultWorkerId = mapTaskResultLocation.getMapTaskResultWorkerId();
        // TODO 暂时使用内存引用,后续替换成RPC调用
        Worker worker = workerIdMap.get(mapTaskResultWorkerId);
        String mapResult = worker.loadMapTaskResultById(mapTaskId);
        return mapResult;
    }

    private String loadMapTaskResultById(String mapTaskId) {
        if (mapTaskId == null) {
            throw new IllegalArgumentException("查询MapId为null");
        }
        if (!mapIdToMapResult.containsKey(mapTaskId)) {
            // 不含对应id
            throw new MissingMapResultException("查询mapId不在本节点上, mapTaskId: " + mapTaskId + "; workerId: " + getWorkerId());
        }
        // TODO 先尝试从内存加载结果，如果内存中没有再尝试从磁盘中加载结果
//        return mapIdToMapResult.get(mapTaskId);
        String read = mapResultManager.read(mapTaskId);
        return read;
    }

    /**
     * 暂定由TaskManager来汇聚各个Worker上的Reduce结果。
     * @param reduceTaskId
     * @return
     */
    public String loadReduceResultById(String reduceTaskId){
        if (reduceTaskId == null) {
            throw new IllegalArgumentException("查询MapId为null");
        }
        if (!reduceIdToReduceResult.containsKey(reduceTaskId)) {
            // 不含对应id
            throw new MissingMapResultException("查询mapId不在本节点上, reduceTaskId: " + reduceTaskId + "; workerId: " + getWorkerId());
        }
        // TODO 先尝试从内存加载结果，如果内存中没有再尝试从磁盘中加载结果
//        return reduceIdToReduceResult.get(reduceTaskId);
        return reduceResultManager.read(reduceTaskId);
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
        private String result;
        private MapTask mapTask;
    }

    @Data
    private static class ReduceTaskResult {
        private String reduceResult;
        private List<String> reduceTaskIds;
    }

}
