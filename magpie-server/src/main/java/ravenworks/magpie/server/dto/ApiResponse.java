package ravenworks.magpie.server.dto;

import java.util.Map;


/**
 * HTTP 接口统一响应信封：成功 success=true 且 error 为 null；失败 success=false 且 data 为 null。
 *
 * @author Raven
 */
public record ApiResponse<T>(boolean success, T data, ErrorInfo error) {

    public static ApiResponse<Map<String, Object>> ok() {
        return new ApiResponse<>(true, Map.of(), null);
    }

    public static ApiResponse<Object> error(String code, String message) {
        return new ApiResponse<>(false, null, new ErrorInfo(code, message));
    }

    /**
     * 错误详情。
     */
    public record ErrorInfo(String code, String message) {

    }

}
