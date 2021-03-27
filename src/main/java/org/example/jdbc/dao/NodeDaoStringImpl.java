package org.example.jdbc.dao;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.xml.datatype.XMLGregorianCalendar;

import org.example.Node;

public class NodeDaoStringImpl extends NodeDao {

    @Override
    public void saveAll(List<Node> nodes) {
        for (Node node : nodes) {
            save(node);
        }
    }

    @Override
    public void save(Node node) {
        try {
            String query = INSERT_STATEMENT;
            query = query.replaceFirst("\\?", safeToString(node.getId().longValue()));
            query = query.replaceFirst("\\?", safeToString(node.getLat()));
            query = query.replaceFirst("\\?", safeToString(node.getLon()));
            query = query.replaceFirst("\\?", safeToString(node.getUser()));
            query = query.replaceFirst("\\?", safeToString(node.getUid().longValue()));
            query = query.replaceFirst("\\?", safeToString(node.isVisible()));
            query = query.replaceFirst("\\?", safeToString(node.getVersion().longValue()));
            query = query.replaceFirst("\\?", safeToString(node.getChangeset().longValue()));
            query = query.replaceFirst("\\?", timeStampToSQLString(node.getTimestamp()));
            Statement statement = connection.createStatement();
            statement.execute(query);
            statement.close();
            saveConnectionsForNode(node);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            throw new RuntimeException(throwables);
        }
    }

    private String safeToString(Object object) {
        if (object == null) {
            return "NULL";
        }
        String s = object.toString();
        if (s.equals(object)) {
            s = "'" + s + "'";
        }
        return s;
    }

    private String timeStampToSQLString(XMLGregorianCalendar timestamp) {
        return String.format("'%d-%d-%d'", timestamp.getYear(), timestamp.getMonth(), timestamp.getDay());
    }
}
