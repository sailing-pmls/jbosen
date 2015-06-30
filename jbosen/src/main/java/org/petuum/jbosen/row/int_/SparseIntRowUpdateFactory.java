package org.petuum.jbosen.row.int_;

import org.petuum.jbosen.row.RowUpdate;
import org.petuum.jbosen.row.RowUpdateFactory;

import java.nio.ByteBuffer;

/**
 * This row update factory produces sparse int row updates.
 */
public class SparseIntRowUpdateFactory implements RowUpdateFactory {

    /**
     * {@inheritDoc}
     */
    @Override
    public RowUpdate createRowUpdate() {
        return new SparseIntRowUpdate();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RowUpdate deserializeRowUpdate(ByteBuffer data) {
        return new SparseIntRowUpdate(data);
    }
}
