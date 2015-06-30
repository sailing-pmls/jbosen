package org.petuum.jbosen.row.long_;

import org.petuum.jbosen.row.Row;
import org.petuum.jbosen.row.RowFactory;

import java.nio.ByteBuffer;

/**
 * This row factory produces sparse long rows.
 */
public class SparseLongRowFactory implements RowFactory {

    /**
     * {@inheritDoc}
     */
    @Override
    public Row createRow() {
        return new SparseLongRow();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Row deserializeRow(ByteBuffer data) {
        return new SparseLongRow(data);
    }
}
