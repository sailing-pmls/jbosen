package org.petuum.jbosen.row.double_;

import org.petuum.jbosen.row.RowUpdate;
import org.petuum.jbosen.row.RowUpdateFactory;

import java.nio.ByteBuffer;

/**
 * This row update factory produces sparse double row updates.
 */
public class SparseDoubleRowUpdateFactory implements RowUpdateFactory {

    /**
     * {@inheritDoc}
     */
    @Override
    public RowUpdate createRowUpdate() {
        return new SparseDoubleRowUpdate();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RowUpdate deserializeRowUpdate(ByteBuffer data) {
        return new SparseDoubleRowUpdate(data);
    }
}
