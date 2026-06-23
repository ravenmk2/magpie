package ravenworks.magpie.server.web;

import io.cloudevents.CloudEvent;
import io.cloudevents.http.HttpMessageFactory;
import lombok.NonNull;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/**
 * Spring 7 兼容的 CloudEvent 转换器：基于 cloudevents-http-basic 解析，
 * 绕开 cloudevents-spring 对 Spring 6 HttpHeaders（实现 Map）的耦合。
 *
 * @author Raven
 */
public class CloudEventHttpMessageConverter extends AbstractHttpMessageConverter<CloudEvent> {

    public CloudEventHttpMessageConverter() {
        super(new MediaType("application", "cloudevents+json"), MediaType.APPLICATION_JSON);
    }

    @Override
    protected boolean supports(@NonNull Class<?> clazz) {
        return CloudEvent.class.isAssignableFrom(clazz);
    }

    @SuppressWarnings("NullableProblems")
    @Override
    protected CloudEvent readInternal(@NonNull Class<? extends CloudEvent> clazz,
                                      @NonNull HttpInputMessage inputMessage) throws IOException {
        Map<String, List<String>> headers = new LinkedHashMap<>();
        inputMessage.getHeaders().forEach(headers::put);
        byte[] body = inputMessage.getBody().readAllBytes();
        try {
            return HttpMessageFactory.createReaderFromMultimap(headers, body).toEvent();
        } catch (RuntimeException e) {
            throw new HttpMessageNotReadableException("Invalid CloudEvent: " + e.getMessage(), e, inputMessage);
        }
    }

    @Override
    public boolean canWrite(@NonNull Class<?> clazz, MediaType mediaType) {
        return false;
    }

    @Override
    protected void writeInternal(@NonNull CloudEvent event,
                                 @NonNull HttpOutputMessage outputMessage) throws HttpMessageNotWritableException {
        throw new HttpMessageNotWritableException("Writing CloudEvent responses is not supported");
    }

}
