package org.coco.boost.core.enums;

/**
 * @author coco
 * @date 2020-07-05 01:39
 **/
public enum SqlMethod {

    COUNT_BY_EXAMPLE("countByExample","根据条件查询数量"),
    DELETE_BY_EXAMPLE("deleteByExample","根据条件删除"),
    DELETE_BY_PRIMARY_KEY("deleteByPrimaryKey","根据主键删除"),
    INSERT("insert","插入"),
    INSERT_LIST("insertList","批量插入"),
    INSERT_SELECTIVE("insertSelective","有选择的插入"),
    SELECT_ALL("selectAll","查询全部"),
    SELECT_BY_EXAMPLE("selectByExample","根据条件查询"),
    SELECT_ONE_BY_EXAMPLE("selectOneByExample","根据条件查询单个"),
    SELECT_BY_EXAMPLE_WITH_BLOBS("selectByExampleWithBLOBs",""),
    SELECT_ONE_BY_EXAMPLE_WITH_BLOBS("selectOneByExampleWithBLOBs","有大字段的时候的根据条件查询单个"),
    SELECT_BY_PRIMARY_KEY("selectByPrimaryKey","根据主键查询"),
    UPDATE_BY_EXAMPLE("updateByExample","根据条件更新"),
    UPDATE_BY_EXAMPLE_SELECTIVE("updateByExampleSelective","根据条件选择性更新"),
    UPDATE_BY_EXAMPLE_WITH_BLOBS("updateByExampleWithBLOBs","有大字段的时候根据条件更新"),
    UPDATE_BY_PRIMARY_KEY("updateByPrimaryKey","根据主键更新"),
    UPDATE_BY_PRIMARY_KEY_SELECTIVE("updateByPrimaryKeySelective","根据主键选择性更新"),
    UPDATE_BY_PRIMARY_KEY_WITH_BLOBS("updateByPrimaryKeyWithBLOBs","有大字段的时候根据主键选择性更新");


    private final String method;
    private final String desc;

    private SqlMethod(String method, String desc) {
        this.method = method;
        this.desc = desc;
    }

    public String getMethod() {
        return this.method;
    }

    public String getDesc() {
        return this.desc;
    }

}
