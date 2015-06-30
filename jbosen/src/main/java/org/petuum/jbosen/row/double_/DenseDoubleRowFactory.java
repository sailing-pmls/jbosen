package org.petuum.jbosen.row.double_;

import org.petuum.jbosen.row.Row;
import org.petuum.jbosen.row.RowFactory;

import java.nio.ByteBuffer;

/**
 * This row factory produces dense double rows of a constant capacity.
 */
public class DenseDoubleRowFactory implements RowFactory {

    private int capacity;

    /**
     * Construct a row factory that produces dense double rows with a certain
     * capacity.
     *
     * @param capacity capacity of rows to produce.
     */
    public DenseDoubleRowFactory(int capacity) {
        assert capacity > 0
                : "Invalid capacity: " + capacity;
        this.capacity = capacity;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Row createRow() {
        return new DenseDoubleRow(this.capacity);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Row deserializeRow(ByteBuffer data) {
        return new DenseDoubleRow(data);
    }
}
