package org.coco.boost.core.service;

import com.github.pagehelper.PageInfo;
import org.coco.boost.core.mapper.BaseMapper;
import org.coco.boost.core.query.BaseExample;
import org.springframework.transaction.annotation.Transactional;

import java.io.Serializable;
import java.util.Collection;
import java.util.Optional;

/**
 * @author coco
 * @date 2020-07-03 12:06
 **/
public interface IService<T,M extends BaseExample> {

    /**
     * 保存单个
     * @param entity
     * @return
     */
    boolean save(T entity);

    @Transactional(
            rollbackFor = {Exception.class}
    )
    default boolean saveList(Collection<T> entityList) {
        return this.saveList(entityList, 1000);
    }

    /**
     * 批量保存
     * @param entityList
     * @param listSize
     * @return
     */
    boolean saveList(Collection<T> entityList, int listSize);

    @Transactional(
            rollbackFor = {Exception.class}
    )
    default boolean saveOrUpdateList(Collection<T> entityList) {
        return this.saveOrUpdateList(entityList, 1000);
    }

    /**
     * 批量 有则更新
     * @param entityList
     * @param listSize
     * @return
     */
    boolean saveOrUpdateList(Collection<T> entityList, int listSize);

    /**
     * 根据id 删除
     * @param id
     * @return
     */
    boolean deleteByKey(Serializable id);

    /**
     * 根据entity的值进行查询删除
     * @param example
     * @return
     */
    boolean deleteByExample(M example);

    /**
     *  批量删除
     * @param keyList
     * @return
     */
    boolean deleteByKeys(Collection<? extends Serializable> keyList);


    /**
     * 根据主键更新
     * @param entity
     * @return
     */
    boolean updateByKey(T entity);

    /**
     *
     * @param entity
     * @param example
     * @return
     */
    boolean update(T entity, M example);

    @Transactional(
            rollbackFor = {Exception.class}
    )
    default boolean updateBykeys(Collection<T> entityList) {
        return this.updateBykeys(entityList, 1000);
    }

    boolean updateBykeys(Collection<T> entityList, int listSize);

    Optional<T> queryByKey(Serializable key);

    Optional<Collection<T>> queryByExample(M example);

    Optional<T> queryOneByExample(M example);

    long count(M example);

    PageInfo<T> selectPageByExample(M example);

    default boolean saveOrUpdate(T entity, M example) {
        return this.update(entity, example) || this.save(entity);
    }

    /**
     * 需要 更新时需要主键
     * @param entity
     * @return
     */
    boolean saveOrUpdate(T entity);


    BaseMapper<T,M> getBaseMapper();

}

