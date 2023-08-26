package master;

import lombok.extern.log4j.Log4j2;
import lombok.extern.slf4j.Slf4j;
import master.enums.TaskStatus;
import tools.SnowflakeGenerator;
import worker.HeartbeatMsg;
import worker.Worker;

import java.util.Collection;
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
    SnowflakeGenerator snowflakeGenerator = new SnowflakeGenerator(200L);
    int chunkSize = 64;
    int taskSize;

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
                if (!isAllMapTaskDone()) {
                    try {
                        Thread.sleep(2000L);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    // start reduce task and stop
                    submitAllReduceTask();
                    return;
                }
            }
        }).start();
    }

    public TaskManager() {

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

    }

    /**
     * 提交reduce任务到worker节点，具体提交节点由TM决定
     */
    private void assignReduceTask(ReduceTask reduceTask) {

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
            mapTask.setTaskId(snowflakeGenerator.generateId());
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

}
