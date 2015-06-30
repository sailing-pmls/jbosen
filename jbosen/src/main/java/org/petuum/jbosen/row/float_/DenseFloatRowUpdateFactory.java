package org.petuum.jbosen.row.float_;

import org.petuum.jbosen.row.RowUpdate;
import org.petuum.jbosen.row.RowUpdateFactory;

import java.nio.ByteBuffer;

/**
 * This row update factory produces dense float row updates of a constant
 * capacity.
 */
public class DenseFloatRowUpdateFactory implements RowUpdateFactory {

    private int capacity;

    /**
     * Construct a row factory that produces dense float row updates with a
     * certain capacity.
     *
     * @param capacity capacity of row updates to produce.
     */
    public DenseFloatRowUpdateFactory(int capacity) {
        assert capacity > 0
                : "Invalid capacity: " + capacity;
        this.capacity = capacity;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RowUpdate createRowUpdate() {
        return new DenseFloatRowUpdate(capacity);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RowUpdate deserializeRowUpdate(ByteBuffer data) {
        return new DenseFloatRowUpdate(data);
    }
}
