package org.apache.dolphinscheduler.plugin.task.etl;

import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;

public class EtlParams extends AbstractParameters {

    private String url;

    private String username;

    private String password;

    private String tenantId;

    private String projectId;

    private String taskId;


    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public boolean checkParameters() {
        return StringUtils.isNotBlank(url)
                && StringUtils.isNotBlank(tenantId)
                && StringUtils.isNotBlank(projectId)
                && StringUtils.isNotBlank(username)
                && StringUtils.isNotBlank(password)
                && StringUtils.isNotBlank(taskId)
                ;
    }
}
