/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.plugin.task.etl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.task.api.*;
import org.apache.dolphinscheduler.plugin.task.api.model.ApplicationInfo;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class EtlTask extends AbstractTask {

    /**
     * output
     */
    protected String output;
    /**
     * etl parameters
     */
    private EtlParams etlParams;
    /**
     * taskExecutionContext
     */
    private TaskExecutionContext taskExecutionContext;

    private boolean isStop = false;

    private String jobId = "";


    private boolean isCancel = false;


    /**
     * constructor
     *
     * @param taskExecutionContext taskExecutionContext
     */
    public EtlTask(TaskExecutionContext taskExecutionContext) {
        super(taskExecutionContext);
        this.taskExecutionContext = taskExecutionContext;
    }

    @Override
    public void init() {
        logger.info("etl task params {}", taskExecutionContext.getTaskParams());
        this.etlParams = JSONUtils.parseObject(taskExecutionContext.getTaskParams(), EtlParams.class);

        if (!etlParams.checkParameters()) {
            throw new RuntimeException("etl task params is not valid");
        }
    }

    @Override
    public void handle(TaskCallBack taskCallBack) throws TaskException {
        //TODO  1, 做digest认证
        try {
            logger.info(" etl task start : {} ", taskExecutionContext.getTaskInstanceId());
            Map<String, Object> runParams = buildRunParams();
            logger.info(" etl task submit request: {} ", JSONUtils.toJsonString(runParams));
            String retJsonStr = HttpClientUtil.sendPost(etlParams.getUrl() + EtlTaskApiEnum.START.getUri(), JSONUtils.toJsonString(runParams));
            logger.info(" etl task submit response: {} ", retJsonStr);

            if (retJsonStr == null) {
                logger.error("etl task submit response is null");
                throw new RuntimeException("Request service exception");
            }
            ObjectNode retJson = JSONUtils.parseObject(retJsonStr);
            if (null == retJson) {
                throw new RuntimeException("Request service exception");
            } else {
                Integer status = retJson.get("code").asInt();
                String msg = retJson.get("msg").asText();
                if (200 != status) {
                    setExitStatusCode(TaskConstants.EXIT_CODE_FAILURE);
                    logger.error(" Startup failed, status code: {}, failed information: {}", status, msg);
                    return;
                }
                JsonNode dataNode = retJson.get("data");
                String processInstanceId = dataNode.get("processInstanceId").asText();
                logger.info("submit etl cluster success ,JobId:{}", processInstanceId);

                this.jobId = processInstanceId;
                taskCallBack.updateRemoteApplicationInfo(taskExecutionContext.getTaskInstanceId(), new ApplicationInfo(processInstanceId));
                setAppIds(jobId);
                // 轮询状态查询接口
                while (!isStop) {
                    JsonNode statusJson = getStatus();
                    if (statusJson == null) {
                        logger.error("etl task status is null, wait 3000ms retry");
                        Thread.sleep(3000L);
                        continue;
                    }
                    Integer runStatus = statusJson.get("state").asInt();
                    /*-1未执行、0失败、1成功、2运行中、3已暂停、4已停止、5排队中*/
                    if (-1 == runStatus || 2 == runStatus || 5 == runStatus) {
                        Thread.sleep(8000L);
                    } else {
                        if (runStatus == 1) {
                            setExitStatusCode(TaskConstants.EXIT_CODE_SUCCESS);
                            logger.info(" Run successfully");
                        } else {
                            setExitStatusCode(TaskConstants.EXIT_CODE_FAILURE);
                            logger.error(" Run failed, status code: {}, exception information: {}", runStatus, statusJson.get("msg").asText());
                        }
                        break;
                    }
                }
                if (isCancel) {
                    logger.info(" The current task is killed... ");
                    Thread.sleep(50000);
                }
                printLog();
            }


        } catch (Exception e) {
            logger.error("etl task failed ", e);
            setExitStatusCode(TaskConstants.EXIT_CODE_FAILURE);
            throw new TaskException("run java task error", e);
        }
    }

    private void printLog() throws IOException {
        JsonNode logJson = getLog();
        if (logJson == null) {
            logger.info(" query etl cluster service log is null");
        } else {
            String message = logJson.get("message").asText();
            logger.info(message);
        }
    }

    private JsonNode getStatus() throws IOException {
        String url = etlParams.getUrl() + EtlTaskApiEnum.STATUS.getUri();
        String params = String.format("?tenantCode=%s&processInstanceId=%s", etlParams.getTenantId(), this.jobId);
        String statusInfo = HttpClientUtil.sendGet(url + params);
        if (null == statusInfo) {
            return null;
        }
        ObjectNode statusNode = JSONUtils.parseObject(statusInfo);
        int code = statusNode.get("code").asInt();
        if (code != 200) {
            return null;
        }
        return statusNode.get("data");
    }

    private JsonNode getLog() throws IOException {
        String url = etlParams.getUrl() + EtlTaskApiEnum.LOG.getUri();
        String params = String.format("?tenantCode=%s&processInstanceId=%s", etlParams.getTenantId(), this.jobId);
        String statusInfo = HttpClientUtil.sendGet(url + params);
        if (null == statusInfo) {
            return null;
        }
        ObjectNode statusNode = JSONUtils.parseObject(statusInfo);
        int code = statusNode.get("code").asInt();
        if (code != 200) {
            return null;
        }
        return statusNode.get("data");
    }


    @Override
    public void cancel() throws TaskException {
        isCancel = true;
        logger.info("cancel etl task ");
        if (isStop) {
            logger.info("task is already stop");
            return;
        }
        isStop = true;
        try {
            String cancelResult = HttpClientUtil.sendPost(etlParams.getUrl() + EtlTaskApiEnum.CANCEL.getUri(), JSONUtils.toJsonString(buildCancelParams()));
            logger.info("cancel etl cluster result :{}", cancelResult);
        } catch (Exception e) {
            logger.error("cancel etl task error：", e);
            return;
        }
        logger.info("cancel etl task success! ");
    }

    private Map<String, Object> buildRunParams() {
        Map<String, Object> runParams = new HashMap<String, Object>();
        runParams.put("tenantCode", etlParams.getTenantId());
        runParams.put("taskId", etlParams.getTaskId());
        runParams.put("params", etlParams.getTaskParams());
        runParams.put("rerun", etlParams.getRerun());
        return runParams;
    }

    private Map<String, Object> buildCancelParams() {
        Map<String, Object> cancelParams = new HashMap<String, Object>();
        cancelParams.put("tenantCode", etlParams.getTenantId());
        cancelParams.put("processInstanceId", jobId);
        return cancelParams;

    }


    @Override
    public AbstractParameters getParameters() {
        return this.etlParams;
    }
}
