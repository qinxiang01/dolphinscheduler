package org.apache.dolphinscheduler.api.service;

import org.apache.dolphinscheduler.dao.vo.ProcessListQueryVo;
import org.apache.dolphinscheduler.dao.entity.User;

import java.util.List;
import java.util.Map;

public interface SdhExtendService {
    Map<String, Object> queryProcessDefinitionByCode(User loginUser, long projectCode, List<ProcessListQueryVo> list);
}
