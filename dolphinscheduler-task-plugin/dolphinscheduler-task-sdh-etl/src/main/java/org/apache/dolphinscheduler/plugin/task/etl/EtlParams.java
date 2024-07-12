package org.apache.dolphinscheduler.plugin.task.etl;

import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;

import java.util.Map;

public class EtlParams extends AbstractParameters {

    private Long clusterId;

    private String url;

    private String username;

    private String password;

    private String tenantId;

    private String projectId;

    private String taskId;

    private Map<String,String> taskParams;

    private int rerun;


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

    public Long getClusterId() {
        return clusterId;
    }

    public void setClusterId(Long clusterId) {
        this.clusterId = clusterId;
    }

    public Map<String, String> getTaskParams() {
        return taskParams;
    }

    public void setTaskParams(Map<String, String> taskParams) {
        this.taskParams = taskParams;
    }

    public int getRerun() {
        return rerun;
    }

    public void setRerun(int rerun) {
        this.rerun = rerun;
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
