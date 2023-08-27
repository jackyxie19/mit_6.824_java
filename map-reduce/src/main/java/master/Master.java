package master;

import client.ClientRequest;
import client.UDF;
import lombok.extern.java.Log;
import lombok.extern.log4j.Log4j2;
import worker.Worker;

import java.util.List;

/**
 * Master暴露对Client和对Worker的交互接口，内部与{@link ResourceManager}等交互的细节在此处屏蔽。
 * 第一版代码通过对象直接调用，后续通过线程+rpc接口模拟分布式场景。
 * JM管理者当前所有作业的执行状态，而TM管理具体任务的执行，RM负责计算资源的申请和放回。
 * 此处不区分Master的客户端和服务端，按理说Master对Client开放的不应该有Worker等调用的RPC。
 */
@Log4j2
public class Master {

    JobManager jobManager;
    ResourceManager resourceManager;

    public Master() {
        jobManager = new JobManager();
        resourceManager = new ResourceManager();
        for (int i = 0; i < 10; i++) {
            Worker worker = new Worker(this);
            resourceManager.registerWorker(worker);
            WorkerInfo workerInfo = worker.askForWorkerInfo();
            resourceManager.updateWorkerInfo(workerInfo);
        }
        resourceManager.postInit();
    }

    /**
     * 开发给Client提交作业申请的接口
     */
    public MasterResponse submitJob(ClientRequest clientRequest) {
        MasterResponse response = new MasterResponse();
        JobManager.JobRequest job = new JobManager.JobRequest();
        // 申请资源
        List<WorkerInfo> workerInfos = resourceManager.applyResource();
        job.setWorkerInfos(workerInfos);
        job.setFileData(clientRequest.getFileData());
        job.setResourceManager(resourceManager);
        job.setMapFunction(clientRequest.getMapFunction());
        job.setReduceFunction(clientRequest.getReduceFunction());
        // 提交给JobManager
        jobManager.submitJob(job);
        // 返回回调给Client
        response.setJobId(job.jobId);
        return response;
    }

    /**
     * 接受Worker节点的心跳数据，在前期的设计中直接worker节点的状态信息随心跳传递。
     */
    public void heartbeatWithTaskInfo(WorkerInfo workerInfo) {
        jobManager.updateJobInfos(workerInfo);
        resourceManager.updateInfo(workerInfo);
        log.info("receive heartbeat from worker : {}", workerInfo);
    }

    /**
     * 开放给客户端的作业详情查询能力
     */
    public void showJobProcedure(String jobId) {

    }

}
