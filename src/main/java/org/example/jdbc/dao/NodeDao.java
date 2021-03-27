package org.example.jdbc.dao;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Optional;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;

import org.example.Node;
import org.example.Tag;
import org.example.jdbs.connection.OSMConnectionProvider;

public abstract class NodeDao {
    protected static final String INSERT_STATEMENT =
        "INSERT INTO nodes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING";
    protected static final String INSERT_CONNECTION_STATEMENT =
        "INSERT INTO node_tags VALUES (?, ?) ON CONFLICT DO NOTHING";

    protected static final String SELECT_STATEMENT = "SELECT * FROM nodes WHERE id = ?";

    protected final PreparedStatement preparedInsert;
    protected final PreparedStatement preparedConnectionInsert;
    protected final PreparedStatement preparedSelect;

    protected final TagDao tagDao = new TagDaoImpl();
    protected final Connection connection;

    protected NodeDao() {
        try {
            this.connection = OSMConnectionProvider.getConnection();
            this.preparedInsert = connection.prepareStatement(INSERT_STATEMENT);
            this.preparedConnectionInsert = connection.prepareStatement(INSERT_CONNECTION_STATEMENT);
            this.preparedSelect = connection.prepareStatement(SELECT_STATEMENT);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            throw new RuntimeException(throwables);
        }
    }

    public abstract void saveAll(List<Node> nodes);

    public void save(Node node) {
        try {
            prepareInsertStatement(node);
            this.preparedInsert.execute();

            saveConnectionsForNode(node);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            throw new RuntimeException(throwables);
        }
    }

    public Optional<Node> getById(long id) {
        try {
            this.preparedSelect.setLong(1, id);
            if (!this.preparedSelect.execute()) {
                return Optional.empty();
            }
            ResultSet resultSet = this.preparedSelect.getResultSet();
            Node node = new Node();
            if (resultSet.next()) {
                node.setId(BigInteger.valueOf(id));
                node.setChangeset(BigInteger.valueOf(resultSet.getLong("changeset")));
                node.setTimestamp(DatatypeFactory.newInstance()
                    .newXMLGregorianCalendar(resultSet.getDate("timestamp").toString()));
                node.setLat(resultSet.getDouble("lat"));
                node.setLon(resultSet.getDouble("lon"));
                node.setUser(resultSet.getString("username"));
                node.setUid(BigInteger.valueOf(resultSet.getLong("uid")));
                node.setVersion(BigInteger.valueOf(resultSet.getLong("version")));
                node.setVisible(resultSet.getBoolean("visible"));
            }
            List<Tag> tags = this.tagDao.getAllByNodeId(id);
            node.getTag().addAll(tags);
            return Optional.of(node);
        } catch (SQLException | DatatypeConfigurationException throwables) {
            throwables.printStackTrace();
            throw new RuntimeException(throwables);
        }
    }

    protected void prepareInsertStatement(Node node) throws SQLException {
        this.preparedInsert.setLong(1, node.getId().longValue());
        setCheckedDouble(preparedInsert, 2, node.getLat());
        setCheckedDouble(preparedInsert, 3, node.getLon());
        this.preparedInsert.setString(4, node.getUser());
        setCheckedLong(this.preparedInsert, 5, node.getUid().longValue());
        setCheckedBoolean(this.preparedInsert, 6, node.isVisible());
        setCheckedLong(this.preparedInsert, 7, node.getVersion().longValue());
        setCheckedLong(this.preparedInsert, 8, node.getChangeset().longValue());
        this.preparedInsert.setDate(9, new Date(node.getTimestamp().toGregorianCalendar().getTimeInMillis()));
    }

    private void setCheckedLong(PreparedStatement preparedStatement, int index, Long value) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(index, Types.BIGINT);
        } else {
            preparedStatement.setLong(index, value);
        }
    }

    private void setCheckedDouble(PreparedStatement preparedStatement, int index, Double value) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(index, Types.DOUBLE);
        } else {
            preparedStatement.setDouble(index, value);
        }
    }

    private void setCheckedBoolean(PreparedStatement preparedStatement, int index, Boolean value) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(index, Types.BOOLEAN);
        } else {
            preparedStatement.setBoolean(index, value);
        }
    }

    protected void saveConnectionsForNode(Node node) {
        try {
            this.tagDao.saveAll(node.getTag());
            for (Tag tag : node.getTag()) {
                this.preparedConnectionInsert.setLong(1, node.getId().longValue());
                this.preparedConnectionInsert.setString(2, tag.getK());
                this.preparedConnectionInsert.execute();
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            throw new RuntimeException(throwables);
        }
    }
}
