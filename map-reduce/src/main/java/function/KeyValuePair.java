package function;

import lombok.Data;

@Data
public class KeyValuePair<K,V> {
    private K key;
    private V value;
}
