package org.petuum.jbosen.row.long_;

import org.petuum.jbosen.row.RowUpdate;

import java.nio.ByteBuffer;

/**
 * This implementation of {@code LongRowUpdate} assumes a sparse set of
 * columns, ie. column IDs can be any int value. In general, this class is less
 * CPU and memory efficient than {@link DenseLongRowUpdate}.
 */
public class SparseLongRowUpdate extends SparseLongColumnMap
        implements LongRowUpdate {

    /**
     * Construct a new object, this object initially contains no columns (ie.
     * all columns are implicitly zero).
     */
    public SparseLongRowUpdate() {
        super();
    }

    /**
     * Copy constructor, constructs a new, deep copy of the argument.
     *
     * @param other object to construct a deep copy of.
     */
    public SparseLongRowUpdate(SparseLongRowUpdate other) {
        super(other);
    }

    /**
     * Construct an object by de-serializing from a {@code ByteBuffer} object.
     *
     * @param buffer the {@code ByteBuffer} containing the serialized data.
     */
    public SparseLongRowUpdate(ByteBuffer buffer) {
        super(buffer);
    }

    /**
     * Returns a deep copy of this row update.
     *
     * @return deep copy of this row update.
     */
    public SparseLongRowUpdate getCopy() {
        return new SparseLongRowUpdate(this);
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
