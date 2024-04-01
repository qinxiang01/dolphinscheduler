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

package org.apache.dolphinscheduler.workflow.engine.workflow;

/**
 * The IWorkflowExecuteRunnable represent a running workflow instance, it is responsible for operate the workflow instance. e.g. start, kill, pause.
 */
public interface IWorkflowExecutionRunnable
        extends
            IWorkflowOuterAction,
            IWorkflowInnerAction,
            IEventfulExecutionRunnable {

    /**
     * Get the identity of the workflow execution runnable.
     *
     * @return the identity of the workflow execution runnable
     */
    IWorkflowExecutionRunnableIdentify getIdentity();

    /**
     * Get the workflow execution context.
     *
     * @return the workflow execution context
     */
    IWorkflowExecutionContext getWorkflowExecutionContext();

}