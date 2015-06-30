package org.petuum.jbosen.row.long_;

import org.petuum.jbosen.row.RowUpdate;
import org.petuum.jbosen.row.RowUpdateFactory;

import java.nio.ByteBuffer;

/**
 * This row update factory produces dense long row updates of a constant
 * capacity.
 */
public class DenseLongRowUpdateFactory implements RowUpdateFactory {

    private int capacity;

    /**
     * Construct a row factory that produces dense long row updates with a
     * certain capacity.
     *
     * @param capacity capacity of row updates to produce.
     */
    public DenseLongRowUpdateFactory(int capacity) {
        assert capacity > 0
                : "Invalid capacity: " + capacity;
        this.capacity = capacity;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RowUpdate createRowUpdate() {
        return new DenseLongRowUpdate(capacity);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RowUpdate deserializeRowUpdate(ByteBuffer data) {
        return new DenseLongRowUpdate(data);
    }
}
