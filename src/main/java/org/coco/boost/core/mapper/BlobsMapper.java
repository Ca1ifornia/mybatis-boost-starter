package org.coco.boost.core.mapper;

import org.apache.ibatis.annotations.Param;
import org.coco.boost.core.query.BaseExample;

import java.util.List;

/**
 * 含有大字段的mapper
 * @author coco
 * @date 2020-07-04 21:01
 **/
public interface BlobsMapper<T,M extends BaseExample> extends BaseMapper{


    List<T> selectByExampleWithBLOBs(M example);

    T selectOneByExampleWithBLOBs(M example);

    int updateByExampleWithBLOBs(@Param("record") T entity, @Param("example") M example);

    int updateByPrimaryKeyWithBLOBs(T entity);

}
