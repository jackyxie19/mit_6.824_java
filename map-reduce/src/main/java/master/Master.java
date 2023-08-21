package master;

import client.UDF;

/**
 * Master暴露对Client和对Worker的交互接口，内部与{@link ResourceManager}等交互的细节在此处屏蔽。
 * 第一版代码通过对象直接调用，后续通过线程+rpc接口模拟分布式场景。
 * JM管理者当前所有作业的执行状态，而TM管理具体任务的执行，RM负责计算资源的申请和放回。
 */
public class Master {

    JobManager jobManager;
    ResourceManager resourceManager;

    /**
     * 开发给Client提交作业申请的接口
     */
    public void submitJob(UDF udf){

    }

    /**
     * 接受Worker节点的心跳数据，在前期的设计中直接worker节点的状态信息随心跳传递。
     */
    public void heartbeatWithTaskInfo(WorkerInfo workerInfo){

    }

    /**
     * 开放给客户端的作业详情查询能力
     */
    public void showJobProcedure(String jobId){

    }

}
