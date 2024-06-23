package cn.org.lizi.util.db;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.sqlite.SQLiteConfig;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @description: TODO sqlite工具类 一系列常用方法
 * @author albert_luo@lizipro.cn
 * @date 2024/6/23 12:56
 * @version 1.0
 */
public class SqliteUtils {
    private static final Logger log = LoggerFactory.getLogger( SqliteUtils.class );
    @Value("${sqlite.url}")
    private static String dbUrl;

    /**
     * @return
     * @description: 获取连接
     */
    public static Connection getConnection(String dbName) {
        Connection connection = null;
        try {
            Class.forName( "org.sqlite.JDBC" );
            SQLiteConfig config = new SQLiteConfig();
            config.setBusyTimeout( 6000 );
            connection = DriverManager.getConnection( "jdbc:sqlite:" +dbUrl+ dbName, config.toProperties() );
        } catch (Exception e) {
            e.printStackTrace();
            log.error( dbName + "sqlite数据库初始化失败：" + e.getMessage() );
        }
        return connection;
    }

    /**
     * @return
     * @description: 检查表是否存在
     */
    public static Boolean checkTableExists(Connection connection, String tableName) {
        Statement stmt;
        String querySql = "select count(*)  from sqlite_master where type='table' and name = '" + tableName + "';";
        try {
            stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery( querySql );
            int count = 0;
            while (rs.next()) {
                count = rs.getInt( 1 );
            }
            rs.close();
            stmt.close();
            if (count > 0) {
                return true;
            }
        } catch (Exception e) {
            log.error( "sqlite判断表结构是否存在异常：" + e.getMessage() );
        }
        return false;
    }


    /**
     * @return
     * @description: 获取数量
     */
    public static Integer executeCount(Connection connection, String sql) {
        Statement stmt;
        try {
            stmt = connection.createStatement();
            ResultSet resultSet = stmt.executeQuery( sql );
            int count = 0;
            while (resultSet.next()) {
                count = resultSet.getInt( 1 );
            }
            resultSet.close();
            stmt.close();
            return count;
        } catch (Exception e) {
            log.error( "sqlite数据库表获取表信息失败：" + e.getMessage() );
        }
        return 0;
    }


    public static List<String> getColNames(Connection connection, String tableName) {
        List<String> colNames = new ArrayList<>();
        Statement st = null;
        ResultSet rs = null;
        try {
            st = connection.createStatement();
            rs = st.executeQuery( "select * from " + tableName );
            ResultSetMetaData metaData = rs.getMetaData();
            int count = metaData.getColumnCount();
            for (int i = 1; i <= count; i++) {
                colNames.add( metaData.getColumnName( i ) );
            }
        } catch (SQLException e) {
            log.error( "sqlite数据库表获取表信息失败：" + e.getMessage() );
        } finally {
            try {
                rs.close();
                st.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return colNames;
    }

    /**
     * 执行sql查询
     *
     * @param sql sql select 语句
     * @return 查询结果
     */
    public static List<Object> queryAll(Connection connection, String sql) {
        List<String> colNames = new ArrayList<>();
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery( sql );
            ResultSetMetaData metaData = resultSet.getMetaData();
            int count = metaData.getColumnCount();
            for (int i = 1; i <= count; i++) {
                colNames.add( metaData.getColumnName( i ) );
            }
            // 将数据存储到数据中
            List<Object> rsList = new ArrayList<Object>();
            Map<String, Object> perMap;
            while (resultSet.next()) {
                perMap = new HashMap<String, Object>();
                for (String columnName : colNames) {
                    // 获取该列对应的值
                    Object value = resultSet.getObject( columnName );
                    perMap.put( columnName, value );
                }
                rsList.add( perMap );
            }
            return rsList;
        } catch (SQLException e) {
            log.error( "sqlite数据库表获取表信息失败：" + e.getMessage() );
        } finally {
            try {
                if (null != resultSet) {
                    resultSet.close();
                }
                if (null != statement) {
                    statement.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }


    /**
     * @return
     * @description: 创建表
     */
    public static Boolean createTable(Connection connection, String sql) {
        Statement stmt;
        try {
            stmt = connection.createStatement();
            stmt.executeUpdate( sql );
            stmt.close();
        } catch (Exception e) {
            try {
                log.error( "sqlite数据库表创建失败：" + e.getMessage() + ",url:" + connection.getMetaData().getURL() );
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            return false;
        }
        return true;
    }

    public static Boolean executeUpdateSql(Connection connection, String sql) {
        Statement stmt;
        try {
            stmt = connection.createStatement();
            stmt.executeUpdate( sql );
            stmt.close();
        } catch (Exception e) {
            try {
                log.error( "sqlite数据库执行sql语句失败：" + e.getMessage() + ",url:" + connection.getMetaData().getURL() );
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            return false;
        }
        return true;
    }


    public static Integer insertOrUpdate(Connection connection, String sql) {
        Statement stmt = null;
        int returns = 0;
        try {
            connection.setAutoCommit( false );
            stmt = connection.createStatement();
            returns = stmt.executeUpdate( sql );
            connection.commit();
            stmt.close();
            return returns;
        } catch (Exception e) {
            try {
                connection.commit();
                if (!isNullOrEmpty( stmt )) {
                    stmt.close();
                }
                log.error( "sqlite数据库执行失败：" + e.getMessage() + ",url:" + connection.getMetaData().getURL() );
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            return returns;
        }
    }


    /**
     * 批量插入数据
     * @param connection 数据库连接
     * @param tableName 表名
     * @param data 数据列表，每个Map表示一行数据，key为列名，value为列值
     * @return 插入的行数
     */
    public static int batchInsert(Connection connection, String tableName, List<Map<String, Object>> data) {
        if (data == null || data.isEmpty()) {
            return 0;
        }
        int count = 0;
        String columns = String.join(", ", data.get(0).keySet());
        String placeholders = String.join(", ", data.get(0).keySet().stream().map(k -> "?").toArray(String[]::new));
        String sql = "INSERT INTO " + tableName + " (" + columns + ") VALUES (" + placeholders + ")";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            connection.setAutoCommit(false);
            for (Map<String, Object> row : data) {
                int index = 1;
                for (String key : row.keySet()) {
                    pstmt.setObject(index++, row.get(key));
                }
                pstmt.addBatch();
                count++;
            }
            pstmt.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            log.error("sqlite批量插入数据失败：" + e.getMessage());
            try {
                connection.rollback();
            } catch (SQLException ex) {
                log.error("sqlite批量插入数据回滚失败：" + ex.getMessage());
            }
        }
        return count;
    }
    /**
     * @return
     * @description: 关闭数据库
     */
    public static Boolean closeConnection(Connection connection) {
        try {
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
            log.error( "sqlite数据库关闭失败：" + e.getMessage() );
            return false;
        }
        return true;
    }


    /**
     * 查询单行记录
     * @param connection 数据库连接
     * @param sql 查询的SQL语句
     * @return 返回结果
     */
    public static Map<String, Object> querySingleRow(Connection connection, String sql) {
        List<Object> resultList = queryAll(connection, sql);
        if (resultList != null && !resultList.isEmpty()) {
            return (Map<String, Object>) resultList.get(0);
        }
        return null;
    }

    /**
     * 查询单个值
     * @param connection 数据库连接
     * @param sql 查询的SQL语句
     * @return 返回结果
     */
    public static Object querySingleValue(Connection connection, String sql) {
        Statement stmt = null;
        ResultSet resultSet = null;
        try {
            stmt = connection.createStatement();
            resultSet = stmt.executeQuery(sql);
            if (resultSet.next()) {
                return resultSet.getObject(1);
            }
        } catch (SQLException e) {
            log.error("sqlite数据库表获取单个值失败：" + e.getMessage());
        } finally {
            try {
                if (resultSet != null) resultSet.close();
                if (stmt != null) stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
    /**
     * 删除数据
     * @param connection 数据库连接
     * @param tableName 表名
     * @param condition 删除条件，例如 "id = 1"
     * @return 删除的行数
     */
    public static int delete(Connection connection, String tableName, String condition) {
        String sql = "DELETE FROM " + tableName + " WHERE " + condition;
        try (Statement stmt = connection.createStatement()) {
            return stmt.executeUpdate(sql);
        } catch (SQLException e) {
            log.error("sqlite删除数据失败：" + e.getMessage());
        }
        return 0;
    }
    /**
     * 更新数据
     * @param connection 数据库连接
     * @param tableName 表名
     * @param data 更新的数据，key为列名，value为列值
     * @param condition 更新条件，例如 "id = 1"
     * @return 更新的行数
     */
    public static int update(Connection connection, String tableName, Map<String, Object> data, String condition) {
        if (data == null || data.isEmpty()) {
            return 0;
        }
        StringBuilder sql = new StringBuilder("UPDATE " + tableName + " SET ");
        for (String key : data.keySet()) {
            sql.append(key).append(" = ?, ");
        }
        sql.delete(sql.length() - 2, sql.length());
        sql.append(" WHERE ").append(condition);
        try (PreparedStatement pstmt = connection.prepareStatement(sql.toString())) {
            int index = 1;
            for (String key : data.keySet()) {
                pstmt.setObject(index++, data.get(key));
            }
            return pstmt.executeUpdate();
        } catch (SQLException e) {
            log.error("sqlite更新数据失败：" + e.getMessage());
        }
        return 0;
    }
    /**
     * 查询表的所有记录
     * @param connection 数据库连接
     * @param tableName 表名
     * @return 返回查询结果
     */
    public static List<Object> queryAllFromTable(Connection connection, String tableName) {
        return queryAll(connection, "SELECT * FROM " + tableName);
    }

    /**
     * 执行SQL脚本
     * @param connection 数据库连接
     * @param sqlScript SQL脚本
     * @return 执行是否成功
     */
    public static boolean executeSqlScript(Connection connection, String sqlScript) {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sqlScript);
            return true;
        } catch (SQLException e) {
            log.error("sqlite执行SQL脚本失败：" + e.getMessage());
        }
        return false;
    }
    public static boolean isNullOrEmpty(Object obj) {
        if (obj == null) {
            return true;
        }
        return isNullOrEmpty(obj.toString());
    }
    public static void main(String[] args) {
        // 获取数据库连接
        Connection connection = SqliteUtils.getConnection("share.db");

        // 创建测试表
        String createTableSql = "CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, name TEXT)";
        SqliteUtils.createTable(connection, createTableSql);

        // 插入测试数据
        List<Map<String, Object>> testData = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("id", 1);
        row1.put("name", "Alice");
        testData.add(row1);

        Map<String, Object> row2 = new HashMap<>();
        row2.put("id", 2);
        row2.put("name", "Bob");
        testData.add(row2);

        SqliteUtils.batchInsert(connection, "test_table", testData);

        // 查询表的所有记录
        List<Object> results = SqliteUtils.queryAllFromTable(connection, "test_table");
            System.out.println(results);
        // 关闭数据库连接
        SqliteUtils.closeConnection(connection);
    }

}
