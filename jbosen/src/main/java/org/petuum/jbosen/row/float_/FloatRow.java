package org.petuum.jbosen.row.float_;

import org.petuum.jbosen.row.Row;

/**
 * This interface specifies a row that contains columns of float values.
 */
public interface FloatRow extends Row {

    /**
     * Get the value associated with a column ID.
     *
     * @param columnId a column ID.
     * @return the value associated with columnId.
     */
    float get(int columnId);

    /**
     * Set the value associated with a column ID.
     *
     * @param columnId a column ID.
     * @param value    the value to be set.
     */
    void set(int columnId, float value);

    /**
     * Increment the value associated with a column ID.
     *
     * @param columnId a column ID.
     * @param value    the value to increment by.
     */
    void inc(int columnId, float value);

    /**
     * Reset all values to the default values.
     */
    void clear();

    /**
     * Obtain an iterator over the columns stored in this object.
     *
     * @return an iterator over this object.
     */
    FloatColumnIterator iterator();

}
