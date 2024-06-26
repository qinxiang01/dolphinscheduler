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

package org.apache.dolphinscheduler.plugin.task.flink;

import lombok.NonNull;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.task.api.AbstractYarnTask;
import org.apache.dolphinscheduler.plugin.task.api.TaskConstants;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.model.ResourceInfo;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;
import org.apache.dolphinscheduler.plugin.task.api.parser.ParameterUtils;
import org.apache.dolphinscheduler.plugin.task.api.utils.LogUtils;
import org.slf4j.Logger;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class FlinkTask extends AbstractYarnTask {

    /**
     * flink parameters
     */
    private FlinkParameters flinkParameters;

    /**
     * taskExecutionContext
     */
    private TaskExecutionContext taskExecutionContext;

    /**
     * rules for flink application ID
     */
    protected static final Pattern FLINK_APPLICATION_REGEX = Pattern.compile(TaskConstants.FLINK_APPLICATION_REGEX);

    private static final Pattern FLINK_JOBID_REGEX = Pattern.compile(TaskConstants.FLINK_APPLICATION_REGEX);

    public FlinkTask(TaskExecutionContext taskExecutionContext) {
        super(taskExecutionContext);
        this.taskExecutionContext = taskExecutionContext;
    }

    @Override
    public void init() {
        logger.info("flink task params {}", taskExecutionContext.getTaskParams());

        flinkParameters = JSONUtils.parseObject(taskExecutionContext.getTaskParams(), FlinkParameters.class);

        if (flinkParameters == null || !flinkParameters.checkParameters()) {
            throw new RuntimeException("flink task params is not valid");
        }
        flinkParameters.setQueue(taskExecutionContext.getQueue());
        setMainJarName();

        FileUtils.generateScriptFile(taskExecutionContext, flinkParameters);
    }

    /**
     * create command
     *
     * @return command
     */
    @Override
    protected String buildCommand() {
        // flink run/run-application [OPTIONS] <jar-file> <arguments>
        List<String> args = FlinkArgsUtils.buildRunCommandLine(taskExecutionContext, flinkParameters);

        String command = ParameterUtils
                .convertParameterPlaceholders(String.join(" ", args), taskExecutionContext.getDefinedParams());

        logger.info("flink task command : {}", command);
        return command;
    }

    @Override
    public void setAppIds(String appIds) {

        List<String> taskJobId = getTaskJobId();
        if (!ObjectUtils.isEmpty(taskJobId)) {
            appIds = appIds + "==" +  String.join(TaskConstants.COMMA, taskJobId);
        }
        super.appIds = appIds;
    }

    @Override
    public List<String> getTaskJobId() {
        return getFlinkJobId(taskRequest.getLogPath(), logger);
    }

    public List<String> getFlinkJobId(@NonNull String logPath, Logger logger) {
        File logFile = new File(logPath);
        if (!logFile.exists() || !logFile.isFile()) {
            return Collections.emptyList();
        }
        Set<String> taskJobIds = new HashSet<>();
        try (Stream<String> stream = Files.lines(Paths.get(logPath))) {
            stream.filter(line -> {
                Matcher matcher = FLINK_JOBID_REGEX.matcher(line);
                return matcher.find();
            }).forEach(line -> {
                Matcher matcher = FLINK_JOBID_REGEX.matcher(line);
                if (matcher.find()) {
                    String taskJobId = matcher.group();
                    if (taskJobIds.add(taskJobId.replace("Job ID: ", ""))) {
                        logger.info("Find taskJobId: {} from {}", taskJobId, logPath);
                    }
                }
            });
            return new ArrayList<>(taskJobIds);
        } catch (IOException e) {
            logger.error("Get taskJobId from log file error, logPath: {}", logPath, e);
            return Collections.emptyList();
        }
    }

    @Override
    protected void setMainJarName() {
        if (flinkParameters.getProgramType() == ProgramType.SQL) {
            logger.info("The current flink job type is SQL, will no need to set main jar");
            return;
        }

        ResourceInfo mainJar = flinkParameters.getMainJar();
        String resourceName = getResourceNameOfMainJar(mainJar);
        mainJar.setRes(resourceName);
        flinkParameters.setMainJar(mainJar);
        logger.info("Success set flink jar: {}", resourceName);
    }

    @Override
    public AbstractParameters getParameters() {
        return flinkParameters;
    }

    /**
     * find app id
     *
     * @param line line
     * @return appid
     */
    protected String findAppId(String line) {
        Matcher matcher = FLINK_APPLICATION_REGEX.matcher(line);
        if (matcher.find()) {
            String str = matcher.group();
            return str.substring(6);
        }
        return null;
    }
}
