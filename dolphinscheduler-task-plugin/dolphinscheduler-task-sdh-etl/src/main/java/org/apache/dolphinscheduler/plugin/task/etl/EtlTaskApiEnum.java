package org.apache.dolphinscheduler.plugin.task.etl;

import org.apache.dolphinscheduler.common.enums.HttpMethod;

public enum EtlTaskApiEnum {

    STATUS("/status", HttpMethod.GET, ""),
    START("/start", HttpMethod.POST, ""),
    CANCEL("/cancel", HttpMethod.POST, ""),
    LOG("/runlog", HttpMethod.GET, ""),
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
