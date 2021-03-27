package org.example.jdbc.dao;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.Node;

public class NodeDaoBatchImpl extends NodeDao {
    static final Logger log = LogManager.getLogger("NodeDaoBatch");

    private static final int CACHE_THRESHOLD = 100;
    private static final Map<Long, Node> localCache = new ConcurrentHashMap<>();

    @Override
    public void saveAll(List<Node> nodes) {
        for (Node node : nodes) {
            save(node);
        }
    }

    @Override
    public void save(Node node) {
        saveToBatch(node);
        if (localCache.size() >= CACHE_THRESHOLD) {
            saveBatch();
        }
    }

    @Override
    public Optional<Node> getById(long id) {
        Node result;
        if ((result = localCache.get(id)) != null) {
            return Optional.of(result);
        }
        return super.getById(id);
    }

    private void saveBatch() {
        synchronized (NodeDaoBatchImpl.class) {
            if (localCache.size() < CACHE_THRESHOLD) {
                return;
            }
            try {
                log.info("Saving batch");
                this.preparedInsert.executeBatch();
                for (Node node : localCache.values()) {
                    saveConnectionsForNode(node);
                }
                localCache.clear();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
                throw new RuntimeException(throwables);
            }
        }
    }

    private void saveToBatch(Node node) {
        try {
            prepareInsertStatement(node);
            this.preparedInsert.addBatch();
            localCache.put(node.getId().longValue(), node);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            throw new RuntimeException(throwables);
        }
    }
}
