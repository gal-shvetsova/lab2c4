package org.example.jdbs.connection;

import java.sql.*;

public class OSMConnectionProvider {
    private static final String LOCAL_DB_URI = "jdbc:postgresql:";
    private static final String LOCAL_DB_USER = "postgres";
    private static final String LOCAL_DB_PASSWORD = "postgres";

    private static Connection connection;

    private OSMConnectionProvider() {}

    public static synchronized Connection getConnection() {
        if (connection == null) {
            try {
                connection = DriverManager.getConnection(LOCAL_DB_URI, LOCAL_DB_USER, LOCAL_DB_PASSWORD);
                createOsmTables(connection);
            } catch (SQLException throwables) {
                throwables.printStackTrace();
                throw new RuntimeException(throwables);
            }
        }
        return connection;
    }

    private static void createOsmTables(Connection connection) throws SQLException {
        String[] createTables = {
                "drop table if exists node_tags",
                "drop table if exists tags",
                "drop table if exists nodes",
                "create table if not exists tags (key varchar(255) primary key, value varchar(255))",
                "create table if not exists nodes (id bigint primary key, lat double precision, lon double precision, username varchar(255), uid bigint, visible boolean, version bigint, changeset bigint, timestamp date)",
                "create table if not exists node_tags (nodeId bigint references nodes (id), tagKey varchar(255) references tags (key), constraint node_tags_pk primary key (nodeId, tagKey))"
        };
        for (String createStatement : createTables) {
            try (PreparedStatement callableStatement = connection.prepareCall(createStatement)) {
                callableStatement.execute();
            }
        }
    }

}
