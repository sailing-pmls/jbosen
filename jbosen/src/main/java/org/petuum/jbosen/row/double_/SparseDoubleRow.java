package org.petuum.jbosen.row.double_;

import org.petuum.jbosen.row.RowUpdate;

import java.nio.ByteBuffer;

/**
 * This implementation of {@code DoubleRow} assumes a sparse set of columns,
 * ie. column IDs can be any int value. In general, this class is less CPU and
 * memory efficient than {@link DenseDoubleRow}.
 */
public class SparseDoubleRow extends SparseDoubleColumnMap
        implements DoubleRow {

    /**
     * Construct a new object, this object initially contains no columns (ie.
     * all columns are implicitly zero).
     */
    public SparseDoubleRow() {
        super();
    }

    /**
     * Copy constructor, constructs a new, deep copy of the argument.
     *
     * @param other object to construct a deep copy of.
     */
    public SparseDoubleRow(SparseDoubleRow other) {
        super(other);
    }

    /**
     * Construct an object by de-serializing from a {@code ByteBuffer} object.
     *
     * @param buffer the {@code ByteBuffer} containing the serialized data.
     */
    public SparseDoubleRow(ByteBuffer buffer) {
        super(buffer);
    }

    /**
     * Returns a deep copy of this row.
     *
     * @return deep copy of this row.
     */
    public SparseDoubleRow getCopy() {
        return new SparseDoubleRow(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void inc(RowUpdate rowUpdate) {
        assert rowUpdate instanceof DoubleColumnMap
                : "Incorrect type for rowUpdate!";
        super.incAll((DoubleColumnMap) rowUpdate);
    }
}
