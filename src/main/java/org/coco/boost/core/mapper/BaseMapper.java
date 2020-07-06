package org.coco.boost.core.mapper;

import org.apache.ibatis.annotations.Param;
import org.coco.boost.core.query.BaseExample;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

/**
 * @author coco
 * @date 2020-07-01 22:56
 **/
public interface BaseMapper<T,M extends BaseExample> extends Mapper<T>{

    int insert(T entity);

    int insertList(Collection<T> list);

    int insertSelective(T entity);

    List<T> selectByExample(M example);

    T selectOneByExample(M example);

    T selectByPrimaryKey(Serializable id);

    int updateByExample(@Param("record")T entity,@Param("example") M exmaple);

    int updateByExampleSelective(@Param("record") T entity, @Param("example") M example);

    int updateByPrimaryKeySelective(T entity);

    int updateByPrimaryKey(T entity);

    int deleteByPrimaryKey(Serializable id);

    int deleteByBatchPrimaryKey(Collection<? extends Serializable> idList);

    int deleteByExample(M example);

    long countByExample(M example);

}
