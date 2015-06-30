package org.petuum.jbosen.row.double_;

import org.petuum.jbosen.row.RowUpdate;

/**
 * This interface specifies a row update that contains columns of double
 * update values.
 */
public interface DoubleRowUpdate extends RowUpdate {

    /**
     * Get the value associated with a column ID.
     *
     * @param columnId a column ID.
     * @return the value associated with columnId.
     */
    double get(int columnId);

    /**
     * Set the value associated with a column ID.
     *
     * @param columnId a column ID.
     * @param value    the value to be set.
     */
    void set(int columnId, double value);

    /**
     * Increment the value associated with a column ID.
     *
     * @param columnId a column ID.
     * @param value    the value to increment by.
     */
    void inc(int columnId, double value);

    /**
     * Reset all values to the default values.
     */
    void clear();

    /**
     * Obtain an iterator over the columns stored in this object.
     *
     * @return an iterator over this object.
     */
    DoubleColumnIterator iterator();

}
