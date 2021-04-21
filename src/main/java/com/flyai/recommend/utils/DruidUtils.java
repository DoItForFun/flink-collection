package com.flyai.recommend.utils;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.flyai.recommend.entity.DataTableEntity;
import com.flyai.recommend.entity.MethodEntity;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.*;

/**
 * @author lizhe
 */
public class DruidUtils {
    private static DruidDataSource dataSource = new DruidDataSource();
    private static Connection connection = null;
    private static void setDataSource() throws Exception {
        try {
            dataSource.setUrl("jdbc:mysql://172.21.16.39:3306/ai_data");
            dataSource.setUsername("cece");
            dataSource.setPassword("e3aXN4my377M8xU?");
            dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
            dataSource.setInitialSize(5);
            dataSource.setMinIdle(20);
            dataSource.setMaxActive(500);
            dataSource.setRemoveAbandoned(true);
            dataSource.setRemoveAbandonedTimeout(30);
            dataSource.setMaxWait(20000);
            dataSource.setTimeBetweenEvictionRunsMillis(20000);
        } catch (Exception e) {
            throw e;
        }
    }

    public static Connection getConnection() {
        try {
            setDataSource();
            connection = dataSource.getConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }


    public static Object selectOne(Class <?> classEntity, Map <String, Object> map) throws Exception {
        String sql = getSelectSql(classEntity, map) + " limit 1";
        if (connection == null || connection.isClosed()) {
            getConnection();
        }
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        Object[] objects = parseDataEntityBeans(resultSet, classEntity);
        if (objects == null || objects.length <= 0) {
            return null;
        }
        return objects[0];
    }

    public static void insertBatch(Class <?> classEntity,List<?> list) throws Exception {
        String batchInsertSql = getBatchInsertSql(classEntity, list);
        if (connection == null || connection.isClosed()) {
            getConnection();
        }
        PreparedStatement preparedStatement = connection.prepareStatement(batchInsertSql);
        preparedStatement.executeUpdate();
    }

    public static void close() {
        if (connection != null) {
            try {
                {
                    if (connection != null) {
                        connection.close();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static String getSelectSql(Class <?> clazz, Map <String, Object> map) {
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

    private static String getBatchInsertSql(Class<?> clazz , List<?> list){
        String table = "insert into " + getTable(clazz);
        StringBuilder fieldsBuffer = new StringBuilder();
        for (Field field : clazz.getDeclaredFields()) {
            if (field != null && !field.toString().isEmpty()) {
                fieldsBuffer.append("`").append(field.getName()).append("`").append(",");
            }
        }
        String fields = fieldsBuffer.toString();
        fields = "(" + fields.substring(0, fields.length() - 1) + ") ";
        StringBuffer values = new StringBuffer().append(" values ");
        for (Object o : list) {
            StringBuilder subValues = new StringBuilder();
            subValues.append("(");
            Field[] f = o.getClass().getDeclaredFields();
            Arrays.stream(f).forEach(field -> {
                //获取是否可访问
                boolean flag = field.isAccessible();
                try {
                    //设置该属性总是可访问
                    field.setAccessible(true);
                    if(field.get(o) instanceof String){
                        subValues.append("'").append(field.get(o)).append("'").append(",");
                    }else{
                        subValues.append(field.get(o)).append(",");
                    }
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
                field.setAccessible(flag);
            });
            String substring = subValues.toString().substring(0, subValues.length() - 1);
            substring += "),";
            values.append(substring);
        }
        String valueString = values.toString().substring(0 , values.length() - 1);
        return table + fields + valueString;
    }

    private static String getInsertSql(Class <?> clazz, Object object) throws Exception {
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


    private static String getColumns(List <String> columnsList) {
        StringBuilder tmpColumns = new StringBuilder();
        for (String s : columnsList) {
            tmpColumns.append(s).append(",");
        }
        if (tmpColumns.length() > 1) {
            return tmpColumns.toString().substring(0, tmpColumns.length() - 1);
        }
        return null;
    }

    private static String getConditionString(Map <String, Object> conditionsMap) {
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

    private static String getTable(Class <?> clazz) {
        return StringUtils.camelToUnderline(clazz.getSimpleName(), "entity", null);
    }

    private static Object[] parseDataEntityBeans(ResultSet rsResult,
                                                 Class <?> classEntity) throws Exception {
        DataTableEntity dataTable = null;
        List <Object> listResult = new ArrayList <>();

        // 获取实体中定义的方法
        HashMap <String, Object> hmMethods = new HashMap <>();

        for (int i = 0; i < classEntity.getDeclaredMethods().length; i++) {
            MethodEntity methodEntity = new MethodEntity();
            // 方法的名称
            String methodName = classEntity.getDeclaredMethods()[i].getName();
            String methodKey = methodName.toUpperCase();
            // 方法的参数
            Class[] paramTypes = classEntity.getDeclaredMethods()[i]
                    .getParameterTypes();

            methodEntity.setMethodName(methodName);
            methodEntity.setMethodParamTypes(paramTypes);

            // 处理方法重载
            if (hmMethods.containsKey(methodKey)) {
                methodEntity.setRepeatMethodNum(methodEntity
                        .getRepeatMethodNum() + 1);
                methodEntity.setRepeatMethodsParamTypes(paramTypes);
            } else {
                hmMethods.put(methodKey, methodEntity);
            }
        }

        // 处理ResultSet结构体信息
        if (rsResult != null) {
            ResultSetMetaData rsMetaData = rsResult.getMetaData();
            int columnCount = rsMetaData.getColumnCount();
            dataTable = new DataTableEntity(columnCount);
            // 获取字段名称，类型
            for (int i = 0; i < columnCount; i++) {
                String columnName = rsMetaData.getColumnName(i + 1);
                int columnType = rsMetaData.getColumnType(i + 1);

                dataTable.setColumnName(columnName, i);
                dataTable.setColumnType(columnType, i);
            }
        }

        // 处理ResultSet数据信息
        while (true) {
            assert rsResult != null;
            if (!rsResult.next()) break;
            // 调用方法，根据字段名在hsMethods中查找对应的set方法
            Object objResult = ParseObjectFromResultSet(rsResult, dataTable,
                    classEntity, hmMethods);
            listResult.add(objResult);
        }

        // 以数组方式返回
        Object objResultArray = Array.newInstance(classEntity,
                listResult.size());
        listResult.toArray((Object[]) objResultArray);

        return (Object[]) objResultArray;
    }

    /**
     * 从ResultSet中解析出单行记录对象，存储在实体对象中
     */
    private static Object ParseObjectFromResultSet(ResultSet rs,
                                                   DataTableEntity dataTable, Class <?> classEntity,
                                                   HashMap <String, Object> hsMethods) throws Exception {
        Object objEntity = classEntity.newInstance();
        Method method = null;

        int nColumnCount = dataTable.getColumnCount();
        String[] strColumnNames = dataTable.getColumnNames();
        for (int i = 0; i < nColumnCount; i++) {
            // 获取字段值
            Object objColumnValue = rs.getObject(strColumnNames[i]);

            // HashMap中的方法名key值
            String strMethodKey = null;

            // 获取set方法名
            if (strColumnNames[i] != null) {
                strMethodKey = String.valueOf("SET"
                        + strColumnNames[i].toUpperCase());
            }
            // 值和方法都不为空,这里方法名不为空即可,值可以为空的
            if (strMethodKey != null) {
                // 判断字段的类型,方法名，参数类型
                try {
                    MethodEntity methodEntity = (MethodEntity) hsMethods
                            .get(strMethodKey);

                    String methodName = methodEntity.getMethodName();
                    int repeatMethodNum = methodEntity.getRepeatMethodNum();

                    Class[] paramTypes = methodEntity.getMethodParamTypes();
                    method = classEntity.getMethod(methodName, paramTypes);
                    // 如果重载方法数 >
                    // 1，则判断是否有java.lang.IllegalArgumentException异常，循环处理
                    try {
                        // 设置参数,实体对象，实体对象方法参数
                        method.invoke(objEntity,
                                objColumnValue);
                    } catch (java.lang.IllegalArgumentException e) {
                        if (i == 0) {
                            System.err.println(e.getMessage());
                        }
                        // 处理重载方法
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
