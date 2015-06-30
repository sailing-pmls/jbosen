package org.petuum.jbosen.row.float_;

import org.petuum.jbosen.row.Row;
import org.petuum.jbosen.row.RowFactory;

import java.nio.ByteBuffer;

/**
 * This row factory produces dense float rows of a constant capacity.
 */
public class DenseFloatRowFactory implements RowFactory {

    private int capacity;

    /**
     * Construct a row factory that produces dense float rows with a certain
     * capacity.
     *
     * @param capacity capacity of rows to produce.
     */
    public DenseFloatRowFactory(int capacity) {
        assert capacity > 0
                : "Invalid capacity: " + capacity;
        this.capacity = capacity;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Row createRow() {
        return new DenseFloatRow(this.capacity);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Row deserializeRow(ByteBuffer data) {
        return new DenseFloatRow(data);
    }
}
