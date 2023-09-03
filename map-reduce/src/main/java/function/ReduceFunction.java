package function;

public abstract class ReduceFunction<K,V> {

   public abstract KeyValuePair<K,V> reduce(KeyValuePair<K,V> reduceOne, KeyValuePair<K,V> other);

   abstract KeyValuePair<K,V> getSum();
}
