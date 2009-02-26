package com.itci.teams;

import java.sql.*;
import java.util.List;
import java.util.ArrayList;


public class DAO {
    private Connection conn;

    public DAO(Connection conn) {
        this.conn = conn;
    }

    public List<String> getAllSchemas() throws SQLException {
        final Statement statement = conn.createStatement();
        final ResultSet resultSet = statement.executeQuery("select distinct owner from all_tables");
        List<String> schemaList = new ArrayList<String>();
        while(resultSet.next()){
            schemaList.add(resultSet.getString(1));
        }
        resultSet.close();
        statement.close();
        return schemaList;
    }

    public List<String> getTables(String schemaName) throws SQLException{
        final PreparedStatement statement = conn.prepareStatement("select table_name from all_tables where owner = ?");
        statement.setString(1, schemaName);

        final ResultSet resultSet = statement.executeQuery();
        List<String> tableList = new ArrayList<String>();
        while(resultSet.next()){
            tableList.add(resultSet.getString(1));
        }
        resultSet.close();
        statement.close();
        return tableList;
    }

    public long countRecords(String schemaName, String tableName) throws SQLException{
        final PreparedStatement statement = conn.prepareStatement("select count(1) from " + schemaName+"."+tableName);
        final ResultSet resultSet = statement.executeQuery();
        long count = -1;
        if(resultSet.next()){
            count = resultSet.getLong(1);
        }
        resultSet.close();
        statement.close();
        return count;
    }
}
