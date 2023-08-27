package exception;

/**
 * worker节点未找到对应数据
 */
public class MissingMapResultException extends RuntimeException{
    public MissingMapResultException() {
        super();
    }

    public MissingMapResultException(String message) {
        super(message);
    }

    public MissingMapResultException(String message, Throwable cause) {
        super(message, cause);
    }

    public MissingMapResultException(Throwable cause) {
        super(cause);
    }
}
