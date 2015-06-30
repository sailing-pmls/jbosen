package org.petuum.jbosen.row.long_;

import org.petuum.jbosen.row.Row;
import org.petuum.jbosen.row.RowFactory;

import java.nio.ByteBuffer;

/**
 * This row factory produces dense long rows of a constant capacity.
 */
public class DenseLongRowFactory implements RowFactory {

    private int capacity;

    /**
     * Construct a row factory that produces dense long rows with a certain
     * capacity.
     *
     * @param capacity capacity of rows to produce.
     */
    public DenseLongRowFactory(int capacity) {
        assert capacity > 0
                : "Invalid capacity: " + capacity;
        this.capacity = capacity;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Row createRow() {
        return new DenseLongRow(this.capacity);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Row deserializeRow(ByteBuffer data) {
        return new DenseLongRow(data);
    }
}
