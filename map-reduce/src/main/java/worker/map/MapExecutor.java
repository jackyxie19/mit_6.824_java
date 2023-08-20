package worker.map;

import base.data.OutputCollector;

public class MapExecutor<K, V> implements MapFunction {
    private MapFunction<K, V> mapFunction;
    private String inputFilePath;
    private OutputCollector<K, V> collector;

    public MapExecutor(MapFunction<K, V> mapFunction, String inputFilePath, OutputCollector<K, V> collector) {
        this.mapFunction = mapFunction;
        this.inputFilePath = inputFilePath;
        this.collector = collector;
    }

    public void execute() {
        // 从inputFilePath读取数据
        // 对每条记录调用mapper.map方法，并将结果存储在collector中

    }

    @Override
    public void map(Object key, Object value, OutputCollector collector) {

    }
}