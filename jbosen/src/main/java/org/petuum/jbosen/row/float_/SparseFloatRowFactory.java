package org.petuum.jbosen.row.float_;

import org.petuum.jbosen.row.Row;
import org.petuum.jbosen.row.RowFactory;

import java.nio.ByteBuffer;

/**
 * This row factory produces sparse float rows.
 */
public class SparseFloatRowFactory implements RowFactory {

    /**
     * {@inheritDoc}
     */
    @Override
    public Row createRow() {
        return new SparseFloatRow();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Row deserializeRow(ByteBuffer data) {
        return new SparseFloatRow(data);
    }
}
