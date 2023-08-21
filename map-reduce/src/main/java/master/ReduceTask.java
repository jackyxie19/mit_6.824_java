package master;

import lombok.Data;

@Data
public class ReduceTask {
    /**
     * Snowflake
     */
    private String taskId;
    private String jobId;
    private String inputLocation;
    private String resultLocation;
    private int status;
    private long submitMillis;
    private long receiveMillis;
    private long startMillis;
    private long endMillis;
    private String workerId;
}
