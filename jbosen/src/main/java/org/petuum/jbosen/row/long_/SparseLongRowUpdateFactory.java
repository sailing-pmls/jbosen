package org.petuum.jbosen.row.long_;

import org.petuum.jbosen.row.RowUpdate;
import org.petuum.jbosen.row.RowUpdateFactory;

import java.nio.ByteBuffer;

/**
 * This row update factory produces sparse long row updates.
 */
public class SparseLongRowUpdateFactory implements RowUpdateFactory {

    /**
     * {@inheritDoc}
     */
    @Override
    public RowUpdate createRowUpdate() {
        return new SparseLongRowUpdate();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RowUpdate deserializeRowUpdate(ByteBuffer data) {
        return new SparseLongRowUpdate(data);
    }
}
