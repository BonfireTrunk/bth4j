package today.bonfire.oss.bth4j;

/**
 * Converts a payload to the text a store keeps and back. {@link TaskType#of(String, Class)} uses the
 * runner's {@link today.bonfire.oss.bth4j.common.JsonMapper}; implement this for another format.
 *
 * @param <T> the payload type
 */
public interface PayloadCodec<T> {

    String encode(T payload) throws Exception;

    T decode(String text) throws Exception;
}
