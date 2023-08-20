package worker.map;

import base.data.OutputCollector;

public class MapExecutor<K, V> implements Mapper{
    private Mapper<K, V> mapper;
    private String inputFilePath;
    private OutputCollector<K, V> collector;

    public MapExecutor(Mapper<K, V> mapper, String inputFilePath, OutputCollector<K, V> collector) {
        this.mapper = mapper;
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