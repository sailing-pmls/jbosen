package org.petuum.jbosen.row.int_;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * This implementation of {@code IntColumnMap} assumes that column IDs are
 * dense, ie. they are contiguous values from 0 to capacity - 1. In general,
 * this class is more CPU and memory efficient than
 * {@link SparseIntColumnMap}.
 */
public class DenseIntColumnMap extends IntColumnMap {

    private int[] values;

    /**
     * Construct a new object based on a capacity. this capacity cannot change
     * throughout the lifetime of this object.
     *
     * @param capacity the capacity of this object, must be strictly positive.
     */
    public DenseIntColumnMap(int capacity) {
        assert capacity > 0
                : "Invalid capacity: " + capacity;
        this.values = new int[capacity];
    }

    /**
     * Copy constructor, constructs a new, deep copy of the argument.
     *
     * @param other object to construct a deep copy of.
     */
    public DenseIntColumnMap(DenseIntColumnMap other) {
        this.values = Arrays.copyOf(other.values, other.values.length);
    }

    /**
     * Construct an object by de-serializing from a {@code ByteBuffer} object.
     *
     * @param buffer the {@code ByteBuffer} containing the serialized data.
     */
    public DenseIntColumnMap(ByteBuffer buffer) {
        int size = buffer.getInt();
        this.values = new int[size];
        for (int i = 0; i < size; i++) {
            values[i] = buffer.getInt();
        }
        assert !buffer.hasRemaining();
    }

    /**
     * The capacity of the object.
     *
     * @return the capacity.
     */
    public int capacity() {
        return values.length;
    }

    /**
     * The backing array of this object, modifying this array will also modify
     * this object.
     *
     * @return the backing array.
     */
    public int[] array() {
        return values;
    }

    /**
     * {@code ByteBuffer} object containing the serialized data for this
     * object.
     *
     * @return serialized data.
     */
    public ByteBuffer serialize() {
        int size = (Integer.SIZE + values.length * Integer.SIZE) / Byte.SIZE;
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putInt(values.length);
        for (int d : values) {
            buffer.putInt(d);
        }
        assert !buffer.hasRemaining();
        return buffer;
    }

    /**
     * {@inheritDoc} This map will always contain column IDs that are from 0 to
     * capacity - 1, and will always not contain any other column IDs.
     */
    @Override
    public boolean contains(int columnId) {
        return 0 <= columnId && columnId < values.length;
    }

    /**
     * {@inheritDoc} Will throw a run-time exception if {@code columnId} is
     * out-of-bounds.
     */
    @Override
    public int get(int columnId) {
        assert columnId >= 0 && columnId < values.length
                : "Index out of range. columnId = " + columnId
                + ", capacity = " + values.length;
        return values[columnId];
    }

    /**
     * {@inheritDoc} Will throw a run-time exception if {@code columnId} is
     * out-of-bounds.
     */
    @Override
    public void set(int columnId, int value) {
        assert columnId >= 0 && columnId < values.length
                : "Index out of range. columnId = " + columnId
                + ", capacity = " + values.length;
        values[columnId] = value;
    }

    /**
     * {@inheritDoc} Will throw a run-time exception if {@code columnId} is
     * out-of-bounds.
     */
    @Override
    public void inc(int columnId, int value) {
        assert columnId >= 0 && columnId < values.length
                : "Index out of range. columnId = " + columnId
                + ", capacity = " + values.length;
        values[columnId] += value;
    }

    /**
     * {@inheritDoc} All columns are set to zero.
     */
    @Override
    public void clear() {
        Arrays.fill(values, 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IntColumnIterator iterator() {
        return new DenseIntColumnIterator();
    }

    private class DenseIntColumnIterator implements IntColumnIterator {

        private int columnId;

        public DenseIntColumnIterator() {
            this.columnId = -1;
        }

        @Override
        public boolean hasNext() {
            return columnId < values.length - 1;
        }

        @Override
        public void advance() {
            assert hasNext()
                    : "Index out of range. columnId = " + columnId
                    + ", capacity = " + values.length;
            columnId++;
        }

        @Override
        public int getColumnId() {
            return columnId;
        }

        @Override
        public int getValue() {
            return values[columnId];
        }

        @Override
        public void setValue(int value) {
            values[columnId] = value;
        }

        @Override
        public void incValue(int value) {
            values[columnId] += value;
        }
    }
}
