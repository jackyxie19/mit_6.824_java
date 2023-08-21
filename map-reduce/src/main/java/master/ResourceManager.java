package master;

/**
 * 计算资源管理，应包含资源的申请、释放、异常清除、状态更新等。
 * 计算资源下发的单位是什么？以单个Worker节点为单位下发（机器级别）、线程级；甚至还可以考虑内存+线程综合指标。
 *
 */
public class ResourceManager {

    /**
     * 随心跳更新状态
     */
    public void updateInfo(WorkerInfo workerInfo){

    }

    /**
     * 任务执行前申请Worker
     */
    public void applyResource(Job job){

    }

    /**
     * JM主动申请归还，如kill事件
     */
    public void releaseResource(){

    }

    private void getTopNFreeWorker(){

    }

    private void assignWorker(){

    }
}
