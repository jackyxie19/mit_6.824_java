package master;

import worker.map.MapExecuteTask;
import worker.reduce.ReduceExecuteTask;
import worker.Worker;

import java.util.ArrayList;
import java.util.List;

/**
 * JM负责多个作业的管理职责，二
 * 1.任务分发策略如何选择？
 * 2.Map任务的分发和Reduce的任务分发隔离开，一个是规整的64m的数据块，reduce的输入块是不确定的。而且分开处理更简单，也更符合单一职责原则。
 * 3.一个JobManager处理多个Job，而一个Job对应一个TaskManager。JM是常驻内存的工作线程，而TM是随作业生灭的实例，当后续也可以考虑把TM池化。
 */
public class JobManager {
    private final List<Worker> workers = new ArrayList<>();

    public void submitJob(List<MapExecuteTask> mapExecuteTasks, List<ReduceExecuteTask> reduceExecuteTasks) {
        // 根据Worker节点的资源情况，将任务分配给不同的Worker
        distributeTasks(mapExecuteTasks, reduceExecuteTasks);

        // 执行任务
    }



    /**
     *
     * @param mapExecuteTasks
     * @param reduceExecuteTasks
     */
    private void distributeTasks(List<MapExecuteTask> mapExecuteTasks, List<ReduceExecuteTask> reduceExecuteTasks) {
        // 实现任务分配逻辑
    }

    private void startTaskManager() {

    }

    /**
     * 更新Job状态
     */
    public void updateJobInfos(){

    }
}