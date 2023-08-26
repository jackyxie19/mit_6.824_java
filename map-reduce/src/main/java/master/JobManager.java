package master;

import client.ClientRequest;
import function.MapFunction;
import function.ReduceFunction;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import lombok.extern.slf4j.Slf4j;
import master.enums.JobStatus;
import tools.SnowflakeGenerator;
import worker.Worker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JM负责多个作业的管理职责，二
 * 1.任务分发策略如何选择？
 * 2.Map任务的分发和Reduce的任务分发隔离开，一个是规整的64m的数据块，reduce的输入块是不确定的。而且分开处理更简单，也更符合单一职责原则。
 * 3.一个JobManager处理多个Job，而一个Job对应一个TaskManager。JM是常驻内存的工作线程，而TM是随作业生灭的实例，当后续也可以考虑把TM池化。
 */
@Log4j2
public class JobManager {

    SnowflakeGenerator snowflakeGenerator = new SnowflakeGenerator(100L);
    private final List<Worker> workers = new ArrayList<>();
    Map<String, JobWithTaskManager> idMapJob = new ConcurrentHashMap<>();

    public void submitJob(JobRequest jobRequest) {
        // 根据Worker节点的资源情况，将任务分配给不同的Worker
        Job job = new Job();
        job.setWorkerInfos(jobRequest.getWorkerInfos());
        job.setFileData(jobRequest.getFileData());
        String jobId = generateJobId();
        job.setJobId(jobId);
        job.setJobStatus(JobStatus.INITIALED.getCode());
        job.setMapTaskList(null);
        job.setReduceTaskList(null);
        job.setMapFunction(jobRequest.getMapFunction());
        job.setReduceFunction(jobRequest.getReduceFunction());
        // 申请资源

        // 启动TM管理任务
        log.info("job:{}", job);
        TaskManager taskManager = new TaskManager(job, jobRequest.getResourceManager());
        JobWithTaskManager jobWithTaskManager = new JobWithTaskManager();
        jobWithTaskManager.setJob(job);
        jobWithTaskManager.setJobId(jobId);
        jobWithTaskManager.setTaskManager(taskManager);
        //
        idMapJob.put(jobId, jobWithTaskManager);
    }

    private String generateJobId() {
        return String.valueOf(snowflakeGenerator.generateId());
    }

    /**
     *
     */
    private void distributeTasks() {
        // 实现任务分配逻辑
    }

    private void startTaskManager() {

    }

    /**
     * 更新Job状态
     */
    public void updateJobInfos(WorkerInfo workerInfo) {

    }

    @Data
    @NoArgsConstructor
    private static class JobWithTaskManager {
        String jobId;
        Job job;
        TaskManager taskManager;
    }

    @Data
    public static class JobRequest {
        String jobId;
        List<WorkerInfo> workerInfos;
        ResourceManager resourceManager;
        String fileData;
        MapFunction mapFunction;
        ReduceFunction reduceFunction;
    }
}