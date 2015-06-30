package org.petuum.jbosen.row.long_;

import org.petuum.jbosen.row.RowUpdate;

import java.nio.ByteBuffer;

/**
 * This implementation of {@code LongRowUpdate} assumes a dense set of
 * columns, ie. they are contiguous values from 0 to capacity - 1. In general,
 * this class is more CPU and memory efficient than
 * {@link SparseLongRowUpdate}.
 */
public class DenseLongRowUpdate extends DenseLongColumnMap
        implements LongRowUpdate {

    /**
     * Construct a row update by specifying a capacity. This capacity cannot
     * change throughout the lifetime of this object.
     *
     * @param capacity capacity of the row update.
     */
    public DenseLongRowUpdate(int capacity) {
        super(capacity);
    }

    /**
     * Copy constructor, constructs a new, deep copy of the argument.
     *
     * @param other row update to construct a deep copy of.
     */
    public DenseLongRowUpdate(DenseLongRowUpdate other) {
        super(other);
    }

    /**
     * Construct a row update by de-serializing from a {@code ByteBuffer}
     * object.
     *
     * @param buffer the {@code ByteBuffer} containing the serialized data.
     */
    public DenseLongRowUpdate(ByteBuffer buffer) {
        super(buffer);
    }

    /**
     * Returns a deep copy of this row update.
     *
     * @return deep copy of this row update.
     */
    public DenseLongRowUpdate getCopy() {
        return new DenseLongRowUpdate(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void inc(RowUpdate rowUpdate) {
        assert rowUpdate instanceof LongColumnMap
                : "Incorrect type for rowUpdate!";
        super.incAll((LongColumnMap) rowUpdate);
    }
}
