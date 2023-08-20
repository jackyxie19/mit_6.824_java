package worker.map;

import base.data.OutputCollector;

public interface MapFunction<K, V> {
    void map(K key, V value, OutputCollector<K, V> collector);
}
