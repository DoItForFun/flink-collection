package com.flyai.recommend.mapper;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.flyai.recommend.config.MysqlClient;
import com.flyai.recommend.entity.*;
import com.flyai.recommend.utils.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.*;

/**
 * @author lizhe
 */
@Slf4j
public class MyTiDbSink<IN> extends RichSinkFunction <IN> {
    private Connection connection;
    private DruidDataSource dataSource = null;

    public MyTiDbSink() {

    }

    public void invoke(IN input) throws Exception {
        if (input instanceof ThreadEventEntity) {
            threadEventProcess(input);
        }
        if (input instanceof ThreadVectorEntity) {
            threadVectorProcess(input);
        }
        if (input instanceof UserEntity) {
            userProcess(input);
        }
        if (input instanceof UserVectorEntity) {
            userVectorProcess(input);
        }
    }

    private void threadEventProcess(IN input) throws Exception {
        ThreadEventEntity threadEventEntity = (ThreadEventEntity) input;
        Map <String, Object> selectMap = new HashMap <>();
        Map <String, Object> conditionMap = new LinkedHashMap <>();
        conditionMap.put("id", threadEventEntity.getId());
        selectMap.put("conditions", conditionMap);
        List <?> list = select(ThreadEventEntity.class, selectMap);
        try{
            if (list == null) {
                insert(ThreadEventEntity.class, threadEventEntity);
            } else {
                if(!list.isEmpty()){
                    ThreadEventEntity dbData = (ThreadEventEntity) list.get(0);
                    Map <String, Object> setMap = JSON.parseObject(JSON.toJSONString(threadEventEntity) , Map.class);
                    if (threadEventEntity.getBrowse() == 1 && dbData.getBrowse() == 0) {
                        setMap.put("browse", 1);
                    }
                    if (threadEventEntity.getDetail() == 1 && dbData.getDetail() == 0) {
                        setMap.put("detail", 1);
                    }
                    if (threadEventEntity.getLike() == 1 && dbData.getLike() == 0) {
                        setMap.put("like", 1);
                    }
                    if (threadEventEntity.getComment() == 1 && dbData.getComment() == 0) {
                        setMap.put("comment", 1);
                    }
                    if (threadEventEntity.getShare() == 1 && dbData.getShare() == 0) {
                        setMap.put("share", 1);
                    }
                    if (threadEventEntity.getCollect() == 1 && dbData.getCollect() == 0) {
                        setMap.put("collect", 1);
                    }
                    if (threadEventEntity.getType() == 1 && dbData.getType() == 0) {
                        setMap.put("type", 1);
                    }
                    if (!setMap.isEmpty()) {
                        setMap.remove("id");
                        selectMap.put("set", setMap);
                        update(ThreadEventEntity.class, selectMap);
                    }
                }
            }
        }catch (Exception e){
            log.error(e.getMessage());
        }

    }

    private void threadVectorProcess(IN input) throws Exception {
        saveById(ThreadVectorEntity.class, JSON.parseObject(JSON.toJSONString(input), Map.class));
    }

    private void userProcess(IN input) throws Exception {
        saveById(UserEntity.class, JSON.parseObject(JSON.toJSONString(input), Map.class));
    }

    private void userVectorProcess(IN input) throws Exception {
        saveById(UserVectorEntity.class, JSON.parseObject(JSON.toJSONString(input), Map.class));
    }

    private void saveById(Class <?> clazz, Map <String, Object> map) throws Exception {
        if (map == null || !map.containsKey("id")) {
            return;
        }
        Map <String, Object> selectMap = new HashMap <>();
        Map <String, Object> conditionMap = new LinkedHashMap <>();
        conditionMap.put("id", map.get("id"));
        selectMap.put("conditions", conditionMap);
        List <?> list = select(clazz, selectMap);
        if (list == null) {
            insert(clazz, JSON.parseObject(JSON.toJSONString(map), clazz));
        } else {
            map.remove("id");
            selectMap.put("set", map);
            System.err.println("update");
            update(clazz, selectMap);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.connection = MysqlClient.getConnect();
    }


    @Override
    public void close() {
        if(this.connection != null){
            MysqlClient.close(this.connection);
        }
    }

    public List <?> select(Class <?> classEntity, Map <String, Object> map) throws Exception {
        String sql = getSelectSql(classEntity, map);
        if (this.connection.isClosed()) {
            this.connection = dataSource.getConnection();
        }
        PreparedStatement preparedStatement = this.connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        Object[] objects = parseDataEntityBeans(resultSet, classEntity);
        preparedStatement.close();
        if (objects.length == 0) {
            return null;
        }
        return Arrays.asList(objects);
    }

    public void insert(Class <?> clazz, Object object) throws Exception {
        String sql = getInsertSql(clazz, object);
        if (this.connection.isClosed()) {
            this.connection = dataSource.getConnection();
        }
        PreparedStatement preparedStatement = this.connection.prepareStatement(sql);
        try {
            preparedStatement.executeUpdate();
            preparedStatement.close();
        } catch (Exception e) {
            System.err.println("insert failed:" + e.getMessage() + ";sql:" + sql);
        }
    }

    public void update(Class <?> classEntity, Map <String, Object> map) throws Exception {
        String sql = getUpdateSql(classEntity, map);
        if (this.connection.isClosed()) {
            this.connection = dataSource.getConnection();
        }
        PreparedStatement preparedStatement = this.connection.prepareStatement(sql);
        try {
            preparedStatement.executeUpdate();
            preparedStatement.close();
        } catch (Exception e) {
            System.err.println("update failed:" + e.getMessage() + ";sql:" + sql);
        }
    }

    private String getInsertSql(Class <?> clazz, Object object) throws Exception {
        String table = "insert into " + getTable(clazz);
        StringBuilder fieldsBuffer = new StringBuilder();
        StringBuilder valuesBuilder = new StringBuilder();
        Map <String, Object> map = JSONObject.parseObject(JSON.toJSONString(object), Map.class);
        for (Field field : clazz.getDeclaredFields()) {
            if (field != null && !field.toString().isEmpty()) {
                fieldsBuffer.append("`").append(field.getName()).append("`").append(",");
                if (map.get(field.getName()) instanceof String) {
                    valuesBuilder.append("'").append(map.get(field.getName()).toString()).append("'").append(",");
                } else {
                    valuesBuilder.append(map.get(field.getName())).append(",");
                }
            }
        }
        String fields = fieldsBuffer.toString();
        fields = "(" + fields.substring(0, fields.length() - 1) + ") ";
        String values = valuesBuilder.toString();
        values = "values(" + values.substring(0, values.length() - 1) + ") ";
        return table + fields + values;
    }

    private String getUpdateSql(Class <?> clazz, Map <String, Object> map) {
        String table = getTable(clazz);
        if (map == null || !map.containsKey("set") || !map.containsKey("conditions")
        ) {
            return null;
        }
        String conditions = getConditionString((Map <String, Object>) map.get("conditions"));
        String updateString = getUpdateColumnsString((Map <String, Object>) map.get("set"));
        return "update " + table + updateString + " where " + conditions;
    }

    private String getSelectSql(Class <?> clazz, Map <String, Object> map) {
        String table = getTable(clazz);
        String columns = " * ";
        String conditions = null;
        if (map != null) {
            if (map.containsKey("conditions")
                    && map.get("conditions") instanceof Map
            ) {
                Map <String, Object> conditionsMap = (Map <String, Object>) map.get("conditions");
                String conditionString = getConditionString(conditionsMap);
                if (conditionString != null) {
                    conditions = conditionString;
                }
            }
            if (map.containsKey("columns")
                    && map.get("columns") instanceof List) {
                List <String> columnsList = (List <String>) map.get("columns");
                String columnsString = getColumns(columnsList);
                if (columnsString != null) {
                    columns = columnsString;
                }
            }
        }
        String sql = "select " + columns + " from " + table;
        if (conditions != null) {
            sql += " where " + conditions;
        }
        return sql;
    }

    private String getUpdateColumnsString(Map <String, Object> updateColumns) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(" ").append("set").append(" ");
        for (Map.Entry <String, Object> stringObjectEntry : updateColumns.entrySet()) {
            stringBuilder.append("`").append(stringObjectEntry.getKey()).append("`").append("=");
            if (stringObjectEntry.getValue() instanceof String) {
                stringBuilder.append("'").append(stringObjectEntry.getValue()).append("'");
            } else {
                stringBuilder.append(stringObjectEntry.getValue());
            }
            stringBuilder.append(",");
        }
        if (stringBuilder.length() > 1) {
            return stringBuilder.toString().substring(0, stringBuilder.length() - 1);
        }
        return null;
    }

    private String getColumns(List <String> columnsList) {
        StringBuilder tmpColumns = new StringBuilder();
        for (String s : columnsList) {
            tmpColumns.append("`").append(s).append("`").append(",");
        }
        if (tmpColumns.length() > 1) {
            return tmpColumns.toString().substring(0, tmpColumns.length() - 1);
        }
        return null;
    }

    private String getConditionString(Map <String, Object> conditionsMap) {
        StringBuilder tmpCondition = new StringBuilder();
        for (Map.Entry <String, Object> condition : conditionsMap.entrySet()) {
            tmpCondition.append(condition.getKey()).append("=");
            if (condition.getValue() instanceof String) {
                tmpCondition.append("'").append(condition.getValue()).append("'");
            } else {
                tmpCondition.append(condition.getValue());
            }
            tmpCondition.append(" and ");
        }
        if (tmpCondition.length() > 5) {
            return tmpCondition.toString().substring(0, tmpCondition.length() - 5);
        }
        return null;
    }

    private String getTable(Class <?> clazz) {
        return StringUtils.camelToUnderline(clazz.getSimpleName(), "entity", null);
    }

    private Object[] parseDataEntityBeans(ResultSet rsResult,
                                          Class <?> classEntity) throws Exception {
        DataTableEntity dataTable = null;
        List <Object> listResult = new ArrayList <>();

        // ??????????????????????????????
        HashMap <String, Object> hmMethods = new HashMap <>();

        for (int i = 0; i < classEntity.getDeclaredMethods().length; i++) {
            MethodEntity methodEntity = new MethodEntity();
            // ???????????????
            String methodName = classEntity.getDeclaredMethods()[i].getName();
            String methodKey = methodName.toUpperCase();
            // ???????????????
            Class[] paramTypes = classEntity.getDeclaredMethods()[i]
                    .getParameterTypes();

            methodEntity.setMethodName(methodName);
            methodEntity.setMethodParamTypes(paramTypes);

            // ??????????????????
            if (hmMethods.containsKey(methodKey)) {
                methodEntity.setRepeatMethodNum(methodEntity
                        .getRepeatMethodNum() + 1);
                methodEntity.setRepeatMethodsParamTypes(paramTypes);
            } else {
                hmMethods.put(methodKey, methodEntity);
            }
        }

        // ??????ResultSet???????????????
        if (rsResult != null) {
            ResultSetMetaData rsMetaData = rsResult.getMetaData();
            int columnCount = rsMetaData.getColumnCount();
            dataTable = new DataTableEntity(columnCount);
            // ???????????????????????????
            for (int i = 0; i < columnCount; i++) {
                String columnName = rsMetaData.getColumnName(i + 1);
                int columnType = rsMetaData.getColumnType(i + 1);

                dataTable.setColumnName(columnName, i);
                dataTable.setColumnType(columnType, i);
            }
        }

        // ??????ResultSet????????????
        while (true) {
            assert rsResult != null;
            if (!rsResult.next()) break;
            // ?????????????????????????????????hsMethods??????????????????set??????
            Object objResult = ParseObjectFromResultSet(rsResult, dataTable,
                    classEntity, hmMethods);
            listResult.add(objResult);
        }

        // ?????????????????????
        Object objResultArray = Array.newInstance(classEntity,
                listResult.size());
        listResult.toArray((Object[]) objResultArray);

        return (Object[]) objResultArray;
    }

    /**
     * ???ResultSet?????????????????????????????????????????????????????????
     */
    private Object ParseObjectFromResultSet(ResultSet rs,
                                            DataTableEntity dataTable, Class <?> classEntity,
                                            HashMap <String, Object> hsMethods) throws Exception {
        Object objEntity = classEntity.newInstance();
        Method method = null;

        int nColumnCount = dataTable.getColumnCount();
        String[] strColumnNames = dataTable.getColumnNames();
        for (int i = 0; i < nColumnCount; i++) {
            // ???????????????
            Object objColumnValue = rs.getObject(strColumnNames[i]);

            // HashMap???????????????key???
            String strMethodKey = null;

            // ??????set?????????
            if (strColumnNames[i] != null) {
                strMethodKey = String.valueOf("SET"
                        + strColumnNames[i].toUpperCase());
            }
            // ????????????????????????,??????????????????????????????,??????????????????
            if (strMethodKey != null) {
                // ?????????????????????,????????????????????????
                try {
                    MethodEntity methodEntity = (MethodEntity) hsMethods
                            .get(strMethodKey);

                    String methodName = methodEntity.getMethodName();
                    int repeatMethodNum = methodEntity.getRepeatMethodNum();

                    Class[] paramTypes = methodEntity.getMethodParamTypes();
                    method = classEntity.getMethod(methodName, paramTypes);
                    // ????????????????????? >
                    // 1?????????????????????java.lang.IllegalArgumentException?????????????????????
                    try {
                        // ????????????,???????????????????????????????????????
                        method.invoke(objEntity,
                                objColumnValue);
                    } catch (java.lang.IllegalArgumentException e) {
                        if (i == 0) {
                            System.err.println(e.getMessage());
                        }
                        // ??????????????????
                        for (int j = 1; j < repeatMethodNum; j++) {
                            try {
                                Class[] repeatParamTypes = methodEntity
                                        .getRepeatMethodsParamTypes(j - 1);
                                method = classEntity.getMethod(methodName,
                                        repeatParamTypes);
                                method.invoke(objEntity,
                                        objColumnValue);
                                break;
                            } catch (java.lang.IllegalArgumentException ex) {
                            }
                        }
                    }
                } catch (NoSuchMethodException e) {
                    throw new NoSuchMethodException();
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
        return objEntity;
    }
}
