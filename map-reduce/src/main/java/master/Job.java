package master;

import function.MapFunction;
import function.ReduceFunction;
import lombok.Data;

import java.util.List;


@Data
public class Job {
    String jobId;
    List<MapTask> mapTaskList;
    List<ReduceTask> reduceTaskList;
    int jobStatus;
    String filePath;
    String fileData;
    List<WorkerInfo> workerInfos;
    MapFunction mapFunction;
    ReduceFunction reduceFunction;
}
