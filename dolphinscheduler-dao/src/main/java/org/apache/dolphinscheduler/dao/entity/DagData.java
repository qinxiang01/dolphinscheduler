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

package org.apache.dolphinscheduler.dao.entity;

import java.util.List;

/**
 * DagData
 */
public class DagData {

    /**
     * processDefinition
     */
    private WorkflowDefinition workflowDefinition;

    /**
     * processTaskRelationList
     */
    private List<WorkflowTaskRelation> workflowTaskRelationList;

    /**
     * processTaskRelationList
     */
    private List<TaskDefinition> taskDefinitionList;

    public DagData(WorkflowDefinition workflowDefinition, List<WorkflowTaskRelation> workflowTaskRelationList,
                   List<TaskDefinition> taskDefinitionList) {
        this.workflowDefinition = workflowDefinition;
        this.workflowTaskRelationList = workflowTaskRelationList;
        this.taskDefinitionList = taskDefinitionList;
    }

    public DagData() {
    }

    public WorkflowDefinition getProcessDefinition() {
        return workflowDefinition;
    }

    public void setProcessDefinition(WorkflowDefinition workflowDefinition) {
        this.workflowDefinition = workflowDefinition;
    }

    public List<WorkflowTaskRelation> getProcessTaskRelationList() {
        return workflowTaskRelationList;
    }

    public void setProcessTaskRelationList(List<WorkflowTaskRelation> workflowTaskRelationList) {
        this.workflowTaskRelationList = workflowTaskRelationList;
    }

    public List<TaskDefinition> getTaskDefinitionList() {
        return taskDefinitionList;
    }

    public void setTaskDefinitionList(List<TaskDefinition> taskDefinitionList) {
        this.taskDefinitionList = taskDefinitionList;
    }
}
