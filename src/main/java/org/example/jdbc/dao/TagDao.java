package org.example.jdbc.dao;

import java.util.List;

import org.example.Tag;

public interface TagDao {
    void saveAll(List<Tag> tags);

    void save(Tag tag);

    List<Tag> getAllByNodeId(long id);
}
