package worker.map;

import base.data.OutputCollector;

public interface Mapper<K, V> {
    void map(K key, V value, OutputCollector<K, V> collector);
}
