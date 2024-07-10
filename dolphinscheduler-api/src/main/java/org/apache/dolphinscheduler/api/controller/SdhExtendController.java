package org.apache.dolphinscheduler.api.controller;


import io.swagger.annotations.*;
import org.apache.dolphinscheduler.api.aspect.AccessLogAnnotation;
import org.apache.dolphinscheduler.api.exceptions.ApiException;
import org.apache.dolphinscheduler.api.service.SdhExtendService;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.dao.vo.ProcessListQueryVo;
import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.dao.entity.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import springfox.documentation.annotations.ApiIgnore;

import java.util.List;
import java.util.Map;

import static org.apache.dolphinscheduler.api.enums.Status.QUERY_DETAIL_OF_PROCESS_DEFINITION_ERROR;

@Api(tags = "SDH-扩展增强接口")
@RestController
@RequestMapping("projects/{projectCode}/sdh/extend")
public class SdhExtendController extends BaseController {

    @Autowired
    private SdhExtendService sdhExtendService;


    /**
     * query detail of process definition by code
     *
     * @param loginUser   login user
     * @param projectCode project code
     * @param list        process definition code and version list
     * @return process definition detail
     */
    @ApiOperation(value = "queryProcessDefinitionByCode", notes = "QUERY_PROCESS_DEFINITION_BY_CODE_NOTES")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "code", value = "PROCESS_DEFINITION_CODE", required = true, dataTypeClass = long.class, example = "123456789")
    })
    @PostMapping(value = "/process/list")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(QUERY_DETAIL_OF_PROCESS_DEFINITION_ERROR)
    @AccessLogAnnotation(ignoreRequestArgs = "loginUser")
    public Result queryProcessDefinitionByCodeAndVersion(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                                         @ApiParam(name = "projectCode", value = "PROJECT_CODE", required = true) @PathVariable long projectCode,
                                                         @RequestBody List<ProcessListQueryVo> list) {
        Map<String, Object> result =
                sdhExtendService.queryProcessDefinitionByCode(loginUser, projectCode, list);
        return returnDataList(result);
    }
}
