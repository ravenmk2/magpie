package ravenworks.magpie.engine.stream;

import lombok.experimental.UtilityClass;

import java.util.Set;

@UtilityClass
public class ReservedHeaders {

    public static final String MSG_TYPE = "msg-type";
    public static final String MSG_EVENT_TIME = "msg-event-time";
    public static final String MSG_TENANT_ID = "msg-tenant-id";
    public static final String MSG_BUSINESS_KEY = "msg-business-key";

    public static final Set<String> HEADERS = Set.of(
            MSG_TYPE,
            MSG_EVENT_TIME,
            MSG_TENANT_ID,
            MSG_BUSINESS_KEY
    );

}
