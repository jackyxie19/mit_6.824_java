package master;

import worker.Worker;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 计算资源管理，应包含资源的申请、释放、异常清除、状态更新等。
 * 计算资源下发的单位是什么？以单个Worker节点为单位下发（机器级别）、线程级；甚至还可以考虑内存+线程综合指标。
 *
 */
public class ResourceManager {

    Map<String, Worker> workerMap;
    /**
     * key是负载，value是节点信息
     */
    TreeMap<Integer, WorkerInfo> workloadMap;
    /**
     * 随心跳更新状态
     */
    public void updateInfo(WorkerInfo workerInfo){

    }

    /**
     * 任务执行前申请Worker
     * v0阶段返回恒定数量的worker节点
     */
    public List<WorkerInfo> applyResource(){
        List<WorkerInfo> res = getTopNFreeWorker(10);

        return res;
    }

    /**
     * JM主动申请归还，如kill事件
     */
    public void releaseResource(){

    }

    /**
     * TODO
     * @param n
     * @return
     */
    private List<WorkerInfo> getTopNFreeWorker(int n){
        synchronized (workloadMap){
            SortedMap<Integer, WorkerInfo> headMap = workloadMap.headMap(n);
            return new ArrayList<>(headMap.values());
        }
    }

    private void assignWorker(){

    }

    public Worker getWorkerById(String workerID){
        return workerMap.get(workerID);
    }
}
