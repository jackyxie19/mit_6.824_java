package master;

import lombok.Data;

import java.util.List;


@Data
public class Job {
    String jobId;
    List<MapTask> mapTaskList;
    List<ReduceTask> reduceTaskList;
    int jobStatus;
}
