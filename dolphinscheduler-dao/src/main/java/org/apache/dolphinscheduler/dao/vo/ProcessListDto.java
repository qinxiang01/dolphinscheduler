package org.apache.dolphinscheduler.dao.vo;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import org.apache.dolphinscheduler.common.enums.WorkflowExecutionStatus;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;

import java.util.Date;

@Data
public class ProcessListDto extends ProcessDefinition {

    private WorkflowExecutionStatus state;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date scheduleTime;
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date startTime;
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date endTime;

    private String duration;
}
