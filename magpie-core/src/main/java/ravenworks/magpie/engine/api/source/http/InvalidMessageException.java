package ravenworks.magpie.engine.api.source.http;


/**
 * 消息字段长度越界：绝不截断（截断会破坏业务关联），入口直接拒绝。
 *
 * @author Raven
 */
public class InvalidMessageException extends RuntimeException {

    public InvalidMessageException(String field, int length, int maxLength) {
        super("Message field '" + field + "' length " + length + " exceeds limit " + maxLength);
    }

}
