package master;

import lombok.Data;
import worker.Worker;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 计算资源管理，应包含资源的申请、释放、异常清除、状态更新等。
 * 计算资源下发的单位是什么？以单个Worker节点为单位下发（机器级别）、线程级；甚至还可以考虑内存+线程综合指标。
 */
public class ResourceManager {

    final Map<String, Worker> workerMap;

    final Map<String, WorkerInfo> workerInfos;

    public ResourceManager() {
        workerMap = new HashMap<>();
        workerInfos = new HashMap<>();
    }

    /**
     * 随心跳更新状态
     */
    public void updateInfo(WorkerInfo workerInfo) {

    }

    /**
     * 任务执行前申请Worker
     * v0阶段返回恒定数量的worker节点
     */
    public List<WorkerInfo> applyResource() {
        List<WorkerInfo> res = getTopNFreeWorker(10);

        return res;
    }

    /**
     * JM主动申请归还，如kill事件
     */
    public void releaseResource() {

    }

    public void updateWorkerInfo(WorkerInfo workerInfo) {
        synchronized (workerInfos) {

            workerInfos.put(workerInfo.getWorkerId(), workerInfo);
        }
    }

    /**
     * TODO
     *
     * @param n
     * @return
     */
    private List<WorkerInfo> getTopNFreeWorker(int n) {
        synchronized (workerInfos) {
            List<WorkerInfo> topNList = workerInfos.values().stream()
                    .sorted(Comparator.comparingLong(WorkerInfo::getNonFinishedTaskNum))
                    .collect(Collectors.toList())
                    .subList(0,n);
            return topNList;
        }
    }

    private void assignWorker() {

    }

    public Worker getWorkerById(String workerID) {
        return workerMap.get(workerID);
    }

    public int registerWorker(Worker worker) {

        String workerId = worker.getWorkerId();
        synchronized (workerMap) {
            if (workerMap.containsKey(workerId)) {
                Worker oldWorker = workerMap.get(workerId);
                if (oldWorker.equals(worker)) {
                    return 0;
                } else {
                    workerMap.put(workerId, worker);
                    return 1;
                }
            }
            workerMap.put(workerId, worker);
        }

        return 0;
    }

    @Data
    private static class WorkerIdWithWorkload implements Comparable<WorkerIdWithWorkload> {
        String workerId;
        int workload;

//        @Override
//        public int compare(WorkerIdWithWorkload o1, WorkerIdWithWorkload o2) {
//            return o1.workload - o2.workload;
//        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WorkerIdWithWorkload that = (WorkerIdWithWorkload) o;

            return Objects.equals(workerId, that.workerId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(workerId);
        }

        static WorkerIdWithWorkload of(WorkerInfo workerInfo) {
            String workerId = workerInfo.getWorkerId();
            WorkerIdWithWorkload workerIdWithWorkload = new WorkerIdWithWorkload();
            workerIdWithWorkload.setWorkerId(workerId);
            long nonFinishedTaskNum = workerInfo.getNonFinishedTaskNum();
            workerIdWithWorkload.setWorkload((int) nonFinishedTaskNum);
            return workerIdWithWorkload;
        }

        @Override
        public int compareTo(WorkerIdWithWorkload o) {
            return this.workload - o.workload;
        }
    }
}
