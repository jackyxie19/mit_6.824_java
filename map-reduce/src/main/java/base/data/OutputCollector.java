package base.data;

public interface OutputCollector<K, V> {
    void collect(K key, V value);
    void emit();
}