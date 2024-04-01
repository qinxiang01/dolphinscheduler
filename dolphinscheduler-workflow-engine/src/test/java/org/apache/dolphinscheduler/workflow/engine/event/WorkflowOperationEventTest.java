package org.apache.dolphinscheduler.workflow.engine.event;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class WorkflowOperationEventTest {

    @Test
    void triggerEvent() {
        WorkflowOperationEvent workflowOperationEvent = WorkflowOperationEvent.triggerEvent(1);
        assertEquals(1, workflowOperationEvent.getWorkflowInstanceId());
        assertEquals(WorkflowOperationEventType.TRIGGER, workflowOperationEvent.getWorkflowOperationType());
    }

    @Test
    void pauseEvent() {
        WorkflowOperationEvent workflowOperationEvent = WorkflowOperationEvent.pauseEvent(1);
        assertEquals(1, workflowOperationEvent.getWorkflowInstanceId());
        assertEquals(WorkflowOperationEventType.PAUSE, workflowOperationEvent.getWorkflowOperationType());
    }

    @Test
    void killEvent() {
        WorkflowOperationEvent workflowOperationEvent = WorkflowOperationEvent.killEvent(1);
        assertEquals(1, workflowOperationEvent.getWorkflowInstanceId());
        assertEquals(WorkflowOperationEventType.KILL, workflowOperationEvent.getWorkflowOperationType());
    }

}