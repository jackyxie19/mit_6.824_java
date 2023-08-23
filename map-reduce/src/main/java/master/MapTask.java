package master;

import function.MapFunction;
import lombok.Data;

@Data
public class MapTask {
    private String taskId;
    private String jobId;
    private String inputData;
    private String inputLocation;
    private String resultLocation;
    private int status;
    private long submitMillis;
    private long receiveMillis;
    private long startMillis;
    private long endMillis;
    private String workerId;
    private MapFunction mapFunction;
}
