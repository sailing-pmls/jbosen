package org.petuum.jbosen.row.int_;

/**
 * This abstract class defines the interface for a structure that contains
 * columns of int primitive types. The access is similar to a map with
 * column IDs as keys and ints as values. The built-in row and row update
 * types are maps of this type.
 */
public abstract class IntColumnMap {

    /**
     * Check if this map contains the specified column.
     *
     * @param columnId the column ID to check.
     * @return whether the map contains the given column ID.
     */
    public abstract boolean contains(int columnId);

    /**
     * Returns the value associated with a particular column ID.
     *
     * @param columnId the column ID.
     * @return the value associated with columnId.
     */
    public abstract int get(int columnId);

    /**
     * Sets the value associated with a particular column ID.
     *
     * @param columnId the column ID.
     * @param value    the value associated with columnId.
     */
    public abstract void set(int columnId, int value);

    /**
     * Increments the current value associated with a column ID.
     *
     * @param columnId the column ID.
     * @param value    the value to increment by.
     */
    public abstract void inc(int columnId, int value);

    /**
     * Increments by all values in a given {@link IntColumnMap}.
     *
     * @param intColumnMap object containing all values to increment by.
     */
    public void incAll(IntColumnMap intColumnMap) {
        IntColumnIterator iter = intColumnMap.iterator();
        while (iter.hasNext()) {
            iter.advance();
            int columnId = iter.getColumnId();
            int value = iter.getValue();
            this.inc(columnId, value);
        }
    }

    /**
     * Resets all values.
     */
    public abstract void clear();

    /**
     * Returns an iterator over the columnId-value pairs contained in this
     * object.
     *
     * @return an iterator object.
     */
    public abstract IntColumnIterator iterator();
}
