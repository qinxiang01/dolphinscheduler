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

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.task.api.AbstractTask;
import org.apache.dolphinscheduler.plugin.task.api.TaskCallBack;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

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
        //TODO  2,发送执行任务的请求，并根据响应信息获取实例Id：jobId
        while (!isStop) {

        }
        //TODO  3，将实例ID发送给给master，存到任务实例表
        //TODO  4、起一个异步线程，定时日志信息，确认日志信息每次是否是全量拉取
        //TODO  5，轮询状态查询接口
        //TODO  6，任务到达最终态，再获取一次日志接口。
        //TODO  7, 任务结束

    }

    @Override
    public void cancel() throws TaskException {
        // TODO 1，做认证，
        // TODO 2,调用停止任务的接口停止任务
    }


    @Override
    public AbstractParameters getParameters() {
        return this.etlParams;
    }
}
