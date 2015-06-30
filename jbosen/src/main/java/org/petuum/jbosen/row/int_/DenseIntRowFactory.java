package org.petuum.jbosen.row.int_;

import org.petuum.jbosen.row.Row;
import org.petuum.jbosen.row.RowFactory;

import java.nio.ByteBuffer;

/**
 * This row factory produces dense int rows of a constant capacity.
 */
public class DenseIntRowFactory implements RowFactory {

    private int capacity;

    /**
     * Construct a row factory that produces dense int rows with a certain
     * capacity.
     *
     * @param capacity capacity of rows to produce.
     */
    public DenseIntRowFactory(int capacity) {
        assert capacity > 0
                : "Invalid capacity: " + capacity;
        this.capacity = capacity;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Row createRow() {
        return new DenseIntRow(this.capacity);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Row deserializeRow(ByteBuffer data) {
        return new DenseIntRow(data);
    }
}
