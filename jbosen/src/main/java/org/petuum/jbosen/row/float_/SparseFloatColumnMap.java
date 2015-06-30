package org.petuum.jbosen.row.float_;

import gnu.trove.TDecorators;
import gnu.trove.iterator.TIntFloatIterator;
import gnu.trove.map.TIntFloatMap;
import gnu.trove.map.hash.TIntFloatHashMap;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * This implementation of {@code FloatColumnMap} assumes that column IDs are
 * sparse, ie. they can be any int value. In general, this class is less CPU
 * and memory efficient than {@link DenseFloatColumnMap}.
 */
public class SparseFloatColumnMap extends FloatColumnMap {

    private TIntFloatMap values;

    /**
     * Construct a new object, this object initially contains no columns.
     */
    public SparseFloatColumnMap() {
        this.values = new TIntFloatHashMap();
    }

    /**
     * Copy constructor, constructs a new, deep copy of the argument.
     *
     * @param other object to construct a deep copy of.
     */
    public SparseFloatColumnMap(SparseFloatColumnMap other) {
        this.values = new TIntFloatHashMap();
        this.values.putAll(other.values);
    }

    /**
     * Construct an object by de-serializing from a {@code ByteBuffer} object.
     *
     * @param buffer the {@code ByteBuffer} containing the serialized data.
     */
    public SparseFloatColumnMap(ByteBuffer buffer) {
        int size = buffer.getInt();
        this.values = new TIntFloatHashMap();
        for (int i = 0; i < size; i++) {
            values.put(buffer.getInt(), buffer.getFloat());
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
    public Map<Integer, Float> map() {
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
                (Integer.SIZE + Float.SIZE)) / Byte.SIZE;
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putInt(values.size());
        TIntFloatIterator it = values.iterator();
        while (it.hasNext()) {
            it.advance();
            buffer.putInt(it.key());
            buffer.putFloat(it.value());
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
    public float get(int columnId) {
        assert values.containsKey(columnId)
                : "Column ID does not exist: " + columnId;
        return values.get(columnId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void set(int columnId, float value) {
        values.put(columnId, value);
    }

    /**
     * {@inheritDoc} If the column does not exist, then this value will be set.
     */
    @Override
    public void inc(int columnId, float value) {
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
    public FloatColumnIterator iterator() {
        return new SparseFloatColumnIterator();
    }

    private class SparseFloatColumnIterator implements FloatColumnIterator {

        private TIntFloatIterator iter;

        public SparseFloatColumnIterator() {
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
        public float getValue() {
            return iter.value();
        }

        @Override
        public void setValue(float value) {
            iter.setValue(value);
        }

        @Override
        public void incValue(float value) {
            iter.setValue(iter.value() + value);
        }
    }
}
