package org.petuum.jbosen.row;

import java.nio.ByteBuffer;

/**
 * This interface specifies a row update for a Bosen PS table. An
 * implementation of this interface does not need to be thread-safe.
 */
public interface RowUpdate {

    /**
     * Increment this row update using a {@code RowUpdate} object.
     *
     * @param rowUpdate the {@code RowUpdate} object to apply to this row
     *                  update.
     */
    void inc(RowUpdate rowUpdate);

    /**
     * Serialize this row update into a {@code ByteBuffer} object.
     *
     * @return a {@code ByteBuffer} object containing the serialized data.
     */
    ByteBuffer serialize();
}