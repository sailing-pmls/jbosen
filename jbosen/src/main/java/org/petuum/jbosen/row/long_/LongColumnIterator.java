package org.petuum.jbosen.row.long_;

/**
 * This interface specifies an iterator over a {@code LongColumnMap} object.
 * Typical usage of this class is as follows:
 * <pre>
 * {@code
 * LongColumnIterator iter = someLongColumnMap.iterator();
 * while (iter.hasNext()) {
 *     iter.advance();
 *     int columnId = iter.getColumnId();
 *     long value = iter.getValue();
 * }
 * }
 * </pre>
 */
public interface LongColumnIterator {

    /**
     * Check whether the iterator has a next element.
     *
     * @return whether or not there is a next element.
     */
    boolean hasNext();

    /**
     * Advance the iterator to the next columnId-value pair. Must be called
     * after instantiation to access the first element.
     */
    void advance();

    /**
     * Returns the columnId of the current columnId-value pair.
     *
     * @return current column ID of the iterator.
     */
    int getColumnId();

    /**
     * Returns the value of the current columnId-value pair.
     *
     * @return current value of the iterator.
     */
    long getValue();

    /**
     * Sets the value of the current columnId-value pair.
     *
     * @param value new value to be set.
     */
    void setValue(long value);

    /**
     * Increments the value of the current columnId-value pair.
     *
     * @param value value to increment by.
     */
    void incValue(long value);
}
