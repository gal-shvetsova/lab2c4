package org.example.jdbc.dao;

import java.util.List;

import org.example.Node;

public class NodeDaoPreparedImpl extends NodeDao {
    @Override
    public void saveAll(List<Node> nodes) {
        for (Node node : nodes) {
            save(node);
        }
    }
}
