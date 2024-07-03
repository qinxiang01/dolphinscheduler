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

package org.apache.dolphinscheduler.server.worker.processor;

import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.Tenant;
import org.apache.dolphinscheduler.plugin.task.api.*;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;
import org.apache.dolphinscheduler.plugin.task.api.parameters.ParametersNode;
import org.apache.dolphinscheduler.plugin.task.api.parameters.resource.ResourceParametersHelper;
import org.apache.dolphinscheduler.plugin.task.api.stream.StreamTask;
import org.apache.dolphinscheduler.remote.command.Command;
import org.apache.dolphinscheduler.remote.command.CommandType;
import org.apache.dolphinscheduler.remote.command.TaskSavePointRequestCommand;
import org.apache.dolphinscheduler.remote.command.TaskSavePointResponseCommand;
import org.apache.dolphinscheduler.remote.processor.NettyRequestProcessor;
import org.apache.dolphinscheduler.server.worker.runner.WorkerManagerThread;
import org.apache.dolphinscheduler.server.worker.runner.WorkerTaskExecuteRunnable;
import org.apache.dolphinscheduler.service.task.TaskPluginManager;
import org.apache.dolphinscheduler.service.utils.LoggerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.TASK_TYPE_DATA_QUALITY;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.TASK_TYPE_K8S;

/**
 * task save point processor
 */
@Component
public class TaskSavePointProcessor implements NettyRequestProcessor {

    private final Logger logger = LoggerFactory.getLogger(TaskSavePointProcessor.class);

    /**
     * task execute manager
     */
    @Autowired
    private WorkerManagerThread workerManager;

    /**
     * task save point process
     *
     * @param channel channel channel
     * @param command command command
     */
    @Override
    public void process(Channel channel, Command command) {
        Preconditions.checkArgument(CommandType.TASK_SAVEPOINT_REQUEST == command.getType(),
                String.format("invalid command type : %s", command.getType()));
        TaskSavePointRequestCommand taskSavePointRequestCommand = JSONUtils.parseObject(command.getBody(), TaskSavePointRequestCommand.class);
        if (taskSavePointRequestCommand == null) {
            logger.error("task savepoint request command is null");
            return;
        }
        logger.info("Receive task savepoint command : {}", taskSavePointRequestCommand);

        int taskInstanceId = taskSavePointRequestCommand.getTaskInstanceId();

        try {
            LoggerUtils.setTaskInstanceIdMDC(taskInstanceId);
            doSavePoint(taskInstanceId);

            sendTaskSavePointResponseCommand(channel, taskInstanceId);
        } finally {
            LoggerUtils.removeTaskInstanceIdMDC();
        }
    }

    private void sendTaskSavePointResponseCommand(Channel channel, int taskInstanceId) {
        TaskSavePointResponseCommand taskSavePointResponseCommand = new TaskSavePointResponseCommand();
        taskSavePointResponseCommand.setTaskInstanceId(taskInstanceId);
        channel.writeAndFlush(taskSavePointResponseCommand.convert2Command()).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (!future.isSuccess()) {
                    logger.error("Submit kill response to master error, kill command: {}", taskSavePointResponseCommand);
                } else
                    logger.info("Submit kill response to master success, kill command: {}", taskSavePointResponseCommand);
            }
        });
    }

    protected void doSavePoint(int taskInstanceId) {
        WorkerTaskExecuteRunnable workerTaskExecuteRunnable = workerManager.getTaskExecuteThread(taskInstanceId);
        if (workerTaskExecuteRunnable == null) {
            logger.warn("taskExecuteThread not found, taskInstanceId:{}", taskInstanceId);
            return;
        }
        AbstractTask task = workerTaskExecuteRunnable.getTask();
        if (task == null) {
            logger.warn("task not found, taskInstanceId:{}", taskInstanceId);
            return;
        }
        if (!(task instanceof StreamTask)) {
            logger.warn("task is not stream task");
            return;
        }
        try {
            ((StreamTask) task).savePoint();
        } catch (Exception e) {
            logger.error("task save point error", e);
        }
    }
}
