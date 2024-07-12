package org.apache.dolphinscheduler.plugin.task.etl;

import org.apache.dolphinscheduler.common.enums.HttpMethod;

public enum EtlTaskApiEnum {

    STATUS("/etl/task/status", HttpMethod.GET, ""),
    START("/etl/task/start", HttpMethod.POST, ""),
    CANCEL("/etl/task/cancel", HttpMethod.POST, ""),
    LOG("/etl/task/log", HttpMethod.GET, ""),
    ;
    private String uri;

    private HttpMethod httpMethod;

    private String requestParams;

    EtlTaskApiEnum(String uri, HttpMethod requestMethod, String requestParams) {
        this.uri = uri;
        this.httpMethod = requestMethod;
        this.requestParams = requestParams;
    }

    public String getUri() {
        return uri;
    }

    public HttpMethod getHttpMethod() {
        return httpMethod;
    }

    public String getRequestParams() {
        return requestParams;
    }
}
