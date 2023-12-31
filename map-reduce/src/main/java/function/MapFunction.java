package function;

/**
 * 1. 使用String类型定义方法
 * 2. 使用泛型定义
 */
public abstract class MapFunction {

    /**
     * 暂时约定行记录的输出以json串的方式输出，
     * @param s 单个chunk块儿。
     * @return
     */
    public abstract String map(String s);
}
