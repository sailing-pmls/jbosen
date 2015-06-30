package org.petuum.jbosen.row.int_;

import org.petuum.jbosen.row.Row;
import org.petuum.jbosen.row.RowFactory;

import java.nio.ByteBuffer;

/**
 * This row factory produces sparse int rows.
 */
public class SparseIntRowFactory implements RowFactory {

    /**
     * {@inheritDoc}
     */
    @Override
    public Row createRow() {
        return new SparseIntRow();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Row deserializeRow(ByteBuffer data) {
        return new SparseIntRow(data);
    }
}
