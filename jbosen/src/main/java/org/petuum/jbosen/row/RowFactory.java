package org.petuum.jbosen.row;

import java.nio.ByteBuffer;

/**
 * This interface specifies a factory for rows. An implementation of this
 * interface must be thread-safe.
 */
public interface RowFactory {

    /**
     * Create and return a new row.
     *
     * @return new row constructed by this factory.
     */
    Row createRow();

    /**
     * Create a new row from serialized data.
     *
     * @param buffer buffer containing serialized data for a row.
     * @return new row constructed from the serialized data.
     */
    Row deserializeRow(ByteBuffer buffer);
}
