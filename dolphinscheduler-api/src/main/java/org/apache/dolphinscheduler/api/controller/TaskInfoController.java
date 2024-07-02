package org.apache.dolphinscheduler.api.controller;

import io.swagger.annotations.*;
import org.apache.dolphinscheduler.api.aspect.AccessLogAnnotation;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ApiException;
import org.apache.dolphinscheduler.api.service.TaskInstanceService;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.mapper.TaskInstanceMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import springfox.documentation.annotations.ApiIgnore;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

import static org.apache.dolphinscheduler.api.enums.Status.QUERY_TASK_LIST_BY_PROCESS_INSTANCE_ID_ERROR;
import static org.apache.dolphinscheduler.common.constants.Constants.DATA_LIST;

@Api(tags = "TASK_INSTANCE_TAG")
@RestController
@RequestMapping("/task-info/")
public class TaskInfoController extends BaseController {
    @Autowired
    TaskInstanceMapper taskInstanceMapper;


    @ApiOperation(value = "queryTaskInstanceInfo", notes = "queryTaskInstanceInfo")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "PROCESS_INSTANCE_ID", required = true, dataTypeClass = int.class, example = "100")
    })
    @GetMapping(value = "/{id}")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(QUERY_TASK_LIST_BY_PROCESS_INSTANCE_ID_ERROR)
    @AccessLogAnnotation(ignoreRequestArgs = "loginUser")
    public Result queryTaskInstanceInfo(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                        @PathVariable("id") Integer id) throws IOException {
        Map<String, Object> result = new HashMap<>();
        TaskInstance taskInstance = taskInstanceMapper.selectById(id);
        result.put(DATA_LIST, taskInstance);
        putMsg(result, Status.SUCCESS);
        return returnDataList(result);
    }

}
