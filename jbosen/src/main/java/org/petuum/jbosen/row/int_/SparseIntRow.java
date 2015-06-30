package org.petuum.jbosen.row.int_;

import org.petuum.jbosen.row.RowUpdate;

import java.nio.ByteBuffer;

/**
 * This implementation of {@code IntRow} assumes a sparse set of columns,
 * ie. column IDs can be any int value. In general, this class is less CPU and
 * memory efficient than {@link DenseIntRow}.
 */
public class SparseIntRow extends SparseIntColumnMap
        implements IntRow {

    /**
     * Construct a new object, this object initially contains no columns (ie.
     * all columns are implicitly zero).
     */
    public SparseIntRow() {
        super();
    }

    /**
     * Copy constructor, constructs a new, deep copy of the argument.
     *
     * @param other object to construct a deep copy of.
     */
    public SparseIntRow(SparseIntRow other) {
        super(other);
    }

    /**
     * Construct an object by de-serializing from a {@code ByteBuffer} object.
     *
     * @param buffer the {@code ByteBuffer} containing the serialized data.
     */
    public SparseIntRow(ByteBuffer buffer) {
        super(buffer);
    }

    /**
     * Returns a deep copy of this row.
     *
     * @return deep copy of this row.
     */
    public SparseIntRow getCopy() {
        return new SparseIntRow(this);
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
