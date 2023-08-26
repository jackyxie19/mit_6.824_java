package worker;

import lombok.Data;

import java.util.List;

@Data
public class HeartbeatMsg {

    private String nodeId;
    private List<String> completedMapTasks;
}
