package org.petuum.jbosen.row.double_;

import org.petuum.jbosen.row.Row;
import org.petuum.jbosen.row.RowFactory;

import java.nio.ByteBuffer;

/**
 * This row factory produces sparse double rows.
 */
public class SparseDoubleRowFactory implements RowFactory {

    /**
     * {@inheritDoc}
     */
    @Override
    public Row createRow() {
        return new SparseDoubleRow();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Row deserializeRow(ByteBuffer data) {
        return new SparseDoubleRow(data);
    }
}
