package org.coco.boost.core.exception;

/**
 * @author coco
 * @date 2020-07-05 00:26
 **/
public class MybatisBoostException extends RuntimeException {

    public MybatisBoostException() {
        super();
    }

    public MybatisBoostException(String message) {
        super(message);
    }

    public MybatisBoostException(String message, Throwable cause) {
        super(message, cause);
    }


    public MybatisBoostException(Throwable cause) {
        super(cause);
    }
}
