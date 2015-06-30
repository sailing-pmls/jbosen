package org.petuum.jbosen.row.int_;

import org.petuum.jbosen.row.RowUpdate;

import java.nio.ByteBuffer;

/**
 * This implementation of {@code IntRow} assumes a dense set of columns, ie.
 * they are contiguous values from 0 to capacity - 1. In general, this class is
 * more CPU and memory efficient than {@link SparseIntRow}.
 */
public final class DenseIntRow extends DenseIntColumnMap implements IntRow {

    /**
     * Construct a row by specifying a capacity. This capacity cannot change
     * throughout the lifetime of this object.
     *
     * @param capacity capacity of the row.
     */
    public DenseIntRow(int capacity) {
        super(capacity);
    }

    /**
     * Copy constructor, constructs a new, deep copy of the argument.
     *
     * @param other row to construct a deep copy of.
     */
    public DenseIntRow(DenseIntRow other) {
        super(other);
    }

    /**
     * Construct a row by de-serializing from a {@code ByteBuffer} object.
     *
     * @param buffer the {@code ByteBuffer} containing the serialized data.
     */
    public DenseIntRow(ByteBuffer buffer) {
        super(buffer);
    }

    /**
     * Returns a deep copy of this row.
     *
     * @return deep copy of this row.
     */
    public DenseIntRow getCopy() {
        return new DenseIntRow(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void inc(RowUpdate rowUpdate) {
        assert rowUpdate instanceof IntColumnMap
                : "Incorrect type for rowUpdate!";
        super.incAll((IntColumnMap) rowUpdate);
    }
}
