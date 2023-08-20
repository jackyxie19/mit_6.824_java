package worker.reduce;

import base.data.OutputCollector;
import base.data.Pair;

import java.util.ArrayList;
import java.util.List;

public class ReduceOutputCollector<K, V> implements OutputCollector<K, V> {
    private List<Pair<K, V>> finalResults = new ArrayList<>();

    @Override
    public void collect(K key, V value) {
        finalResults.add(new Pair<>(key, value));
    }

    @Override
    public void emit() {
        // 在Reduce阶段完成后，将最终结果进行输出
        for (Pair<K, V> result : finalResults) {
            // 输出最终结果
        }
    }
}