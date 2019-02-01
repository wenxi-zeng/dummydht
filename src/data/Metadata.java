package data;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Metadata {

    private DatabaseMetaData databaseMetaData;
    private String schema;
    private Database db;

    public Metadata(Connection conn, String schema) {
        this.schema = schema;
        db = new Database(conn);
        try {
            databaseMetaData = (DatabaseMetaData) conn.getMetaData();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public List<String> getTableNames() {
        List<String> tables = new ArrayList<String>();
        try {
            ResultSet resultSet = (ResultSet) databaseMetaData.getTables(null,
                    schema, "%", null);
            while (resultSet.next()) {
                tables.add(resultSet.getString(3));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return tables;
    }

    public List<String> getColumnNames(String table) {
        List<String> columns = new ArrayList<String>();
        try {
            ResultSet resultSet = (ResultSet) databaseMetaData.getColumns(null,
                    schema, table, null);
            while (resultSet.next()) {
                columns.add(resultSet.getString("COLUMN_NAME"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return columns;
    }

    public List<String> getSQLDataTypeNames(String table) {
        List<String> columns = new ArrayList<String>();
        try {
            ResultSet resultSet = (ResultSet) databaseMetaData.getColumns(null,
                    schema, table, null);
            while (resultSet.next()) {
                columns.add(resultSet.getString("TYPE_NAME"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return columns;
    }

    public List<String> getColumnClassNames(String table) {
        List<String> classes = new ArrayList<String>();
        db.limit(0);
        ResultSetMetaData metadata;
        try {
            metadata = (ResultSetMetaData) db.get(table).getMetaData();
            for (int i = 1; i <= metadata.getColumnCount(); i++) {
                classes.add(metadata.getColumnClassName(i));
            }
        } catch (SQLException e1) {
            e1.printStackTrace();
        }
        return classes;
    }

    public String getPrimaryKey(String table) {
        String pk = null;
        try {
            ResultSet resultSet = (ResultSet) databaseMetaData.getPrimaryKeys(
                    null, schema, table);
            if (resultSet.next()) {
                pk = resultSet.getString("COLUMN_NAME");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return pk;
    }

    public Map<String, String> getForeignKeys(String table) {
        Map<String, String> fks = new HashMap<String, String>();
        try {
            ResultSet resultSet = (ResultSet) databaseMetaData.getExportedKeys(
                    null, schema, table);
            while (resultSet.next()) {
                fks.put(resultSet.getString("FKTABLE_NAME"),
                        resultSet.getString("FKCOLUMN_NAME"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return fks;
    }
}
