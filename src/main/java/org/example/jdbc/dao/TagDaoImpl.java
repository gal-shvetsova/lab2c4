package org.example.jdbc.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.example.Tag;
import org.example.jdbs.connection.OSMConnectionProvider;

public class TagDaoImpl implements TagDao {
    private static final String INSERT_STATEMENT = "INSERT INTO tags VALUES (?, ?) ON CONFLICT DO NOTHING";

    private static final String SELECT_FROM_NODE = "SELECT * FROM tags JOIN node_tags ON tagkey = key WHERE nodeid = ?";

    private final PreparedStatement preparedInsert;
    private final PreparedStatement preparedSelect;

    public TagDaoImpl() {
        try {
            Connection connection = OSMConnectionProvider.getConnection();
            this.preparedInsert = connection.prepareStatement(INSERT_STATEMENT);
            this.preparedSelect = connection.prepareStatement(SELECT_FROM_NODE);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            throw new RuntimeException(throwables);
        }
    }

    @Override
    public void saveAll(List<Tag> tags) {
        for (Tag tag : tags) {
            save(tag);
        }
    }

    @Override
    public void save(Tag tag) {
        try {
            this.preparedInsert.setString(1, tag.getK());
            this.preparedInsert.setString(2, tag.getV());
            this.preparedInsert.execute();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            throw new RuntimeException(throwables);
        }
    }

    @Override
    public List<Tag> getAllByNodeId(long id) {
        try {
            List<Tag> result = new ArrayList<>();
            this.preparedSelect.setLong(1, id);
            if (this.preparedSelect.execute()) {
                ResultSet resultSet = this.preparedSelect.getResultSet();
                while (resultSet.next()) {
                    Tag tag = new Tag();
                    tag.setK(resultSet.getString("key"));
                    tag.setV(resultSet.getString("value"));
                    result.add(tag);
                }
                resultSet.close();
            }
            return result;
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            throw new RuntimeException(throwables);
        }
    }
}
