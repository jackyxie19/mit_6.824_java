package master;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.log4j.Log4j2;
import master.enums.TaskStatus;
import tools.SnowflakeGenerator;
import worker.HeartbeatMsg;
import worker.Worker;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 作业管理：
 * 1.一个Job一个TaskManger
 * 2.判断job的整体进展，map任务提交->map任务完成->reduce任务提交->reduce任务完成。
 */

@Log4j2
public class TaskManager {
    JSONObject properties;
    String fileData;
    Map<String, MapTask> idToMapTask = new ConcurrentHashMap<>();
    Map<String, ReduceTask> idToReduceTask = new ConcurrentHashMap<>();
    ResourceManager resourceManager;
    List<String> workerIdList;
    /**
     * 记录每个Worker的已完成任务
     */
    private Map<String, List<String>> workerTasks = new HashMap<>();

    Job job;
    String jobId;
    final SnowflakeGenerator taskIdGenerator;
    final SnowflakeGenerator reduceIdGenerator;
    int chunkSize = 64;
    int taskSize;

    volatile boolean allMapDone = false;
    volatile boolean reduceTaskSubmitted = false;

    public TaskManager(Job job, ResourceManager resourceManager) {
        this();
        // 初始化任务
        this.resourceManager = resourceManager;
        fileData = job.getFileData();
        this.job = job;
        this.jobId = job.getJobId();
        workerIdList = job.getWorkerInfos().stream().map(WorkerInfo::getWorkerId).collect(Collectors.toList());
        submitAllMapTask();

        // 监听map任务是否全部完成
        new Thread(() -> {
            while (true) {
                if (allMapDone) {
                    // 所有Map任务完成
                    if (reduceTaskSubmitted) {
                        // 已提交reduce作业，可跳出
                        return;
                    }
                    // 提交作业
                    submitAllReduceTask();
                    reduceTaskSubmitted = true;
                } else {
                    // 继续监听map任务是否全部完成
                    allMapDone = isAllMapTaskDone();
                    try {
                        Thread.sleep(2000L);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }).start();

        // 启动线程监听ReduceTask状态
        new Thread(() -> {
            while (true) {
                try {
                    if (!reduceTaskSubmitted) {
                        // 任务未提交，休眠后继续循环
                        Thread.sleep(2000L);
                        continue;
                    }
                    // 监听任务是否完成
                    if (isAllReduceTaskDone()) {
                        // 汇聚reduce结果并返回client
                        Map<String,Object> reduceResult = new HashMap<>();
                        idToReduceTask.forEach((reduceTaskId, reduceTask)->{
                            String workerId = reduceTask.getWorkerId();
                            Worker worker = resourceManager.getWorkerById(workerId);
                            String reduceTaskResult = worker.loadReduceResultById(reduceTaskId);
                            JSONObject jsonObject = JSON.parseObject(reduceTaskResult);
                            reduceResult.putAll(jsonObject.getInnerMap());
                        });
                        log.info("jobId:{}, reduce result:{}",jobId,JSON.toJSON(reduceResult));
                        // 处理完成，跳出
                        // TODO 销毁TM，以及将结果落盘
                        return;
                    } else {
                        // 再次检查Reduce任务是否完成
                        Thread.sleep(2000L);
                    }
                } catch (Exception e) {
                    log.error("监听Reduce任务状态异常", e);
                }

            }
        }).start();
    }


    public TaskManager() {
        // 对象实例标识
        int objectLocal = this.hashCode() & 0x3FF;
        taskIdGenerator = new SnowflakeGenerator(objectLocal - 1);
        reduceIdGenerator = new SnowflakeGenerator(objectLocal - 2);
    }


    /**
     * 将单个作业分发到worker上
     *
     * @param task 待提交map任务
     */
    private void assignMapTask(MapTask task) {
        Worker worker = resourceManager.getWorkerById(task.getWorkerId());
        worker.submitMapTask(task);
    }

    private void submitAllReduceTask() {
        // 指定reduceTask数量、每个reduceTask负责的id范围，以及MapTaskId与ReduceTaskId的映射。
        // 按workerId聚合MapTask
        Map<String, List<MapTask>> workerIdToMapTasks = idToMapTask.values().stream().collect(Collectors.groupingBy(MapTask::getWorkerId));
        List<MapTaskResultLocation> resultLocations = idToMapTask.values().stream().map(e -> {
            String mapTaskId = e.getTaskId();
            String workerId = e.getWorkerId();
            MapTaskResultLocation resultLocation = new MapTaskResultLocation();
            resultLocation.setMapTaskResultWorkerId(workerId);
            resultLocation.setMapTaskId(mapTaskId);
            return resultLocation;
        }).collect(Collectors.toList());
        int workerSize = workerIdList.size();

        // TODO 此处暂时固定reduce数量为4.
        int reduceTaskSize = 4;
        for (int i = 0; i < reduceTaskSize; i++) {
            ReduceTask reduceTask = new ReduceTask();
            reduceTask.setJobId(jobId);
            String reduceTaskId = reduceIdGenerator.generateIdWithSpin();
            reduceTask.setTaskId(reduceTaskId);
            reduceTask.setMapTaskResultLocations(resultLocations);
            reduceTask.setDesignatedKeyHash(i);
            String workerId = assignWorker();
            reduceTask.setWorkerId(workerId);
            assignReduceTask(reduceTask);
            idToReduceTask.put(reduceTaskId, reduceTask);

        }
    }

    /**
     * 提交reduce任务到worker节点，具体提交节点由TM决定
     */
    private void assignReduceTask(ReduceTask reduceTask) {
        Worker worker = resourceManager.getWorkerById(reduceTask.getWorkerId());
        worker.submitReduceTask(reduceTask);
    }

    /**
     * 作业启动时对Job中的Map任务进行切分
     */
    private void submitAllMapTask() {
        int length = fileData.length();
        int size = length / chunkSize + 1;
        this.taskSize = size;
        for (int i = 0; i < size; i++) {
            int rightIndex = Math.min(length, (i + 1) * chunkSize);
            String chunk = fileData.substring(i, rightIndex);
            MapTask mapTask = new MapTask();
            mapTask.setTaskId(taskIdGenerator.generateIdWithSpin());
            mapTask.setJobId(jobId);
            mapTask.setWorkerId(assignWorker());
//            mapTask.setInputLocation(chunk);
            mapTask.setInputData(chunk);
            mapTask.setMapFunction(job.getMapFunction());
            idToMapTask.put(mapTask.getTaskId(), mapTask);
            log.info("tm submit task : {}", mapTask.getTaskId());
            assignMapTask(mapTask);
        }
    }

    private String assignWorker() {
        return workerIdList.get((int) (System.currentTimeMillis() % workerIdList.size()));
    }

    public void receiveHeartbeat(HeartbeatMsg heartbeatMessage) {
        String workerId = heartbeatMessage.getNodeId();
        List<String> completedMapTasks = heartbeatMessage.getCompletedMapTasks();

        // 更新Worker的已完成任务列表
        workerTasks.put(workerId, completedMapTasks);

        // 在这里根据任务完成情况，可以判断是否需要调度Reduce任务
    }

    /**
     * TODO
     */
    private void shuffle() {

    }

    /**
     * TODO
     */
    private void retryMapTask() {

    }

    /**
     * TODO
     */
    private void retryReduceTask() {

    }

    /**
     * TODO
     */
    private void destroy() {

    }

    /**
     * JM更新信息调用，更新各worker节点上当前TM所属任务的状态。
     */
    public void updateTaskInfo(WorkerInfo workerInfo) {

    }

    /**
     * 整个作业级别的Kill
     * TODO
     */
    public void jobKill() {

    }

    /**
     * Worker上运行着同Job的不同Task，所以Worker不感知Job对应的MapTask及ReduceTask是否完成。
     * 需要TaskManager判断单个作业的整体进程。
     *
     * @return
     */
    private boolean isAllMapTaskDone() {
        Collection<MapTask> mapTasks = idToMapTask.values();
        for (MapTask mapTask : mapTasks) {
            if (mapTask.getStatus() != TaskStatus.FINISHED.getCode()) {
                return false;
            }
        }
        return true;
    }

    private boolean isAllReduceTaskDone() {
        Collection<ReduceTask> reduceTasks = idToReduceTask.values();
        for (ReduceTask reduceTask : reduceTasks) {
            if (reduceTask.getStatus() != TaskStatus.FINISHED.getCode()) {
                return false;
            }

        }
        return true;
    }

}
