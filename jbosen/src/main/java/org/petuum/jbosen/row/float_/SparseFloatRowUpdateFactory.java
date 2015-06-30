package org.petuum.jbosen.row.float_;

import org.petuum.jbosen.row.RowUpdate;
import org.petuum.jbosen.row.RowUpdateFactory;

import java.nio.ByteBuffer;

/**
 * This row update factory produces sparse float row updates.
 */
public class SparseFloatRowUpdateFactory implements RowUpdateFactory {

    /**
     * {@inheritDoc}
     */
    @Override
    public RowUpdate createRowUpdate() {
        return new SparseFloatRowUpdate();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RowUpdate deserializeRowUpdate(ByteBuffer data) {
        return new SparseFloatRowUpdate(data);
    }
}
