package org.apache.dolphinscheduler.api.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.service.*;
import org.apache.dolphinscheduler.dao.vo.ProcessListDto;
import org.apache.dolphinscheduler.dao.vo.ProcessListQueryVo;
import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.dao.entity.*;
import org.apache.dolphinscheduler.dao.mapper.*;
import org.apache.dolphinscheduler.dao.repository.ProcessDefinitionDao;
import org.apache.dolphinscheduler.service.process.ProcessService;
import org.apache.dolphinscheduler.service.task.TaskPluginManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import java.util.List;
import java.util.Map;

import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.WORKFLOW_DEFINITION;

@Service
public class SdhExtendServiceImpl extends BaseServiceImpl implements SdhExtendService {
    @Autowired
    private ProjectMapper projectMapper;

    @Autowired
    private ProjectService projectService;

    @Autowired
    TaskDefinitionLogMapper taskDefinitionLogMapper;

    @Autowired
    private SdhExtendMapper sdhExtendMapper;

    @Override
    public Map<String, Object> queryProcessDefinitionByCode(User loginUser, long projectCode, List<ProcessListQueryVo> list) {
        Project project = projectMapper.queryByCode(projectCode);
        // check user access for project
        Map<String, Object> result =
                projectService.checkProjectAndAuth(loginUser, project, projectCode, WORKFLOW_DEFINITION);
        if (result.get(Constants.STATUS) != Status.SUCCESS) {
            return result;
        }

        LambdaQueryWrapper<ProcessDefinition> queryWrapper = new LambdaQueryWrapper<>();
        for (ProcessListQueryVo queryVo : list) {
            queryWrapper.or(wrapper -> wrapper.eq(ProcessDefinition::getCode, queryVo.getCode()).eq(ProcessDefinition::getVersion, queryVo.getVersion()));
        }

        List<ProcessListDto> retList = sdhExtendMapper.selectInfoAndLastInstanceInfo(projectCode, list);

        result.put(Constants.DATA_LIST, retList);
        putMsg(result, Status.SUCCESS);
        return result;
    }
}
