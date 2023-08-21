package client;

import function.MapFunction;
import function.ReduceFunction;
import lombok.Data;

@Data
public class UDF {
    private MapFunction mapFunction;
    private ReduceFunction reduceFunction;
}
