package org.petuum.jbosen.row.double_;

import org.petuum.jbosen.row.RowUpdate;

import java.nio.ByteBuffer;

/**
 * This implementation of {@code DoubleRowUpdate} assumes a sparse set of
 * columns, ie. column IDs can be any int value. In general, this class is less
 * CPU and memory efficient than {@link DenseDoubleRowUpdate}.
 */
public class SparseDoubleRowUpdate extends SparseDoubleColumnMap
        implements DoubleRowUpdate {

    /**
     * Construct a new object, this object initially contains no columns (ie.
     * all columns are implicitly zero).
     */
    public SparseDoubleRowUpdate() {
        super();
    }

    /**
     * Copy constructor, constructs a new, deep copy of the argument.
     *
     * @param other object to construct a deep copy of.
     */
    public SparseDoubleRowUpdate(SparseDoubleRowUpdate other) {
        super(other);
    }

    /**
     * Construct an object by de-serializing from a {@code ByteBuffer} object.
     *
     * @param buffer the {@code ByteBuffer} containing the serialized data.
     */
    public SparseDoubleRowUpdate(ByteBuffer buffer) {
        super(buffer);
    }

    /**
     * Returns a deep copy of this row update.
     *
     * @return deep copy of this row update.
     */
    public SparseDoubleRowUpdate getCopy() {
        return new SparseDoubleRowUpdate(this);
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
