package org.petuum.jbosen.row.int_;

import org.petuum.jbosen.row.RowUpdate;

import java.nio.ByteBuffer;

/**
 * This implementation of {@code IntRowUpdate} assumes a sparse set of
 * columns, ie. column IDs can be any int value. In general, this class is less
 * CPU and memory efficient than {@link DenseIntRowUpdate}.
 */
public class SparseIntRowUpdate extends SparseIntColumnMap
        implements IntRowUpdate {

    /**
     * Construct a new object, this object initially contains no columns (ie.
     * all columns are implicitly zero).
     */
    public SparseIntRowUpdate() {
        super();
    }

    /**
     * Copy constructor, constructs a new, deep copy of the argument.
     *
     * @param other object to construct a deep copy of.
     */
    public SparseIntRowUpdate(SparseIntRowUpdate other) {
        super(other);
    }

    /**
     * Construct an object by de-serializing from a {@code ByteBuffer} object.
     *
     * @param buffer the {@code ByteBuffer} containing the serialized data.
     */
    public SparseIntRowUpdate(ByteBuffer buffer) {
        super(buffer);
    }

    /**
     * Returns a deep copy of this row update.
     *
     * @return deep copy of this row update.
     */
    public SparseIntRowUpdate getCopy() {
        return new SparseIntRowUpdate(this);
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
