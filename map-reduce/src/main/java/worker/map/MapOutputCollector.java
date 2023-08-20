package worker.map;

import base.data.OutputCollector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapOutputCollector<K, V> implements OutputCollector<K, V> {
    private final Map<K, List<V>> intermediateData = new HashMap<>();

    @Override
    public void collect(K key, V value) {
        intermediateData.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }

    @Override
    public void emit() {
        // Map阶段的OutputCollector不需要立即进行输出
        // 中间数据将在Shuffle阶段后被分发给Reduce任务
    }

    public Map<K, List<V>> getIntermediateData() {
        return intermediateData;
    }
}