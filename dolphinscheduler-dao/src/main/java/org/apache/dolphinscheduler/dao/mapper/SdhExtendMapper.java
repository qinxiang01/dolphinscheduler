package org.apache.dolphinscheduler.dao.mapper;

import org.apache.dolphinscheduler.dao.vo.ProcessListDto;
import org.apache.dolphinscheduler.dao.vo.ProcessListQueryVo;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface SdhExtendMapper {


    List<ProcessListDto> selectInfoAndLastInstanceInfo(@Param("projectCode") long projectCode, @Param("list") List<ProcessListQueryVo> list);
}
