package client;

import function.MapFunction;
import function.ReduceFunction;
import lombok.Data;

@Data
public class ClientRequest {
    String filePath;
    String fileData;
    long submitMillis;
    MapFunction mapFunction;
    ReduceFunction reduceFunction;
}
