package ravenworks.magpie.server.controller;

import io.cloudevents.CloudEvent;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.*;
import ravenworks.magpie.engine.source.http.HttpSourceRouter;
import ravenworks.magpie.engine.source.http.NoSubscriberException;
import ravenworks.magpie.engine.source.http.TopicNotAllowedException;
import ravenworks.magpie.server.dto.ErrorResponse;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;


/**
 * @author Raven
 */
@RestController
@RequestMapping("/api/v1/publish")
@RequiredArgsConstructor
public class PublishController {

    @NonNull
    private final HttpSourceRouter router;

    @PostMapping("/{source}")
    public CompletableFuture<ResponseEntity<Object>> publish(@PathVariable String source,
                                                             @RequestBody CloudEvent event) {
        return this.router.publish(source, event)
                .handle((v, error) -> error == null ? success() : toError(error));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<Object> onUnreadable(HttpMessageNotReadableException e) {
        Object body = new ErrorResponse("invalid_request_error", e.getMessage());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(body);
    }

    private static ResponseEntity<Object> success() {
        Object body = Map.of();
        return ResponseEntity.ok(body);
    }

    private static ResponseEntity<Object> toError(Throwable error) {
        Throwable cause = error instanceof CompletionException && error.getCause() != null
                ? error.getCause()
                : error;

        HttpStatus status;
        String code;
        if (cause instanceof TopicNotAllowedException) {
            status = HttpStatus.FORBIDDEN;
            code = "topic_not_allowed_error";
        } else if (cause instanceof NoSubscriberException) {
            status = HttpStatus.SERVICE_UNAVAILABLE;
            code = "no_subscriber_error";
        } else {
            status = HttpStatus.BAD_GATEWAY;
            code = "publish_failed_error";
        }

        Object body = new ErrorResponse(code, cause.getMessage());
        return ResponseEntity.status(status).body(body);
    }

}
