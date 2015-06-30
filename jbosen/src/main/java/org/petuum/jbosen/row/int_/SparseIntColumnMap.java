package org.petuum.jbosen.row.int_;

import gnu.trove.TDecorators;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * This implementation of {@code IntColumnMap} assumes that column IDs are
 * sparse, ie. they can be any int value. In general, this class is less CPU
 * and memory efficient than {@link DenseIntColumnMap}.
 */
public class SparseIntColumnMap extends IntColumnMap {

    private TIntIntMap values;

    /**
     * Construct a new object, this object initially contains no columns.
     */
    public SparseIntColumnMap() {
        this.values = new TIntIntHashMap();
    }

    /**
     * Copy constructor, constructs a new, deep copy of the argument.
     *
     * @param other object to construct a deep copy of.
     */
    public SparseIntColumnMap(SparseIntColumnMap other) {
        this.values = new TIntIntHashMap();
        this.values.putAll(other.values);
    }

    /**
     * Construct an object by de-serializing from a {@code ByteBuffer} object.
     *
     * @param buffer the {@code ByteBuffer} containing the serialized data.
     */
    public SparseIntColumnMap(ByteBuffer buffer) {
        int size = buffer.getInt();
        this.values = new TIntIntHashMap();
        for (int i = 0; i < size; i++) {
            values.put(buffer.getInt(), buffer.getInt());
        }
        assert !buffer.hasRemaining();
    }

    /**
     * The number of columns this map contains.
     *
     * @return number of columns this map contains.
     */
    public int size() {
        return values.size();
    }

    /**
     * The backing map of this object, modifying this map will also modify this
     * object.
     *
     * @return the backing map.
     */
    public Map<Integer, Integer> map() {
        return TDecorators.wrap(values);
    }

    /**
     * {@code ByteBuffer} object containing the serialized data for this
     * object.
     *
     * @return serialized data.
     */
    public ByteBuffer serialize() {
        int size = (Integer.SIZE + values.size() *
                (Integer.SIZE + Integer.SIZE)) / Byte.SIZE;
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putInt(values.size());
        TIntIntIterator it = values.iterator();
        while (it.hasNext()) {
            it.advance();
            buffer.putInt(it.key());
            buffer.putInt(it.value());
        }
        assert !buffer.hasRemaining();
        return buffer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains(int columnId) {
        return values.containsKey(columnId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int get(int columnId) {
        assert values.containsKey(columnId)
                : "Column ID does not exist: " + columnId;
        return values.get(columnId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void set(int columnId, int value) {
        values.put(columnId, value);
    }

    /**
     * {@inheritDoc} If the column does not exist, then this value will be set.
     */
    @Override
    public void inc(int columnId, int value) {
        values.adjustOrPutValue(columnId, value, value);
    }

    /**
     * {@inheritDoc} All columns are removed from this map.
     */
    @Override
    public void clear() {
        values.clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IntColumnIterator iterator() {
        return new SparseIntColumnIterator();
    }

    private class SparseIntColumnIterator implements IntColumnIterator {

        private TIntIntIterator iter;

        public SparseIntColumnIterator() {
            this.iter = values.iterator();
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public void advance() {
            assert iter.hasNext()
                    : "Index out of range.";
            iter.advance();
        }

        @Override
        public int getColumnId() {
            return iter.key();
        }

        @Override
        public int getValue() {
            return iter.value();
        }

        @Override
        public void setValue(int value) {
            iter.setValue(value);
        }

        @Override
        public void incValue(int value) {
            iter.setValue(iter.value() + value);
        }
    }
}
