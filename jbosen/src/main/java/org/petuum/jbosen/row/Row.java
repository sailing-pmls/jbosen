package org.petuum.jbosen.row;

import java.nio.ByteBuffer;

/**
 * This interface specifies a row in a Bosen PS table. An implementation of
 * this interface does not need to be thread-safe.
 */
public interface Row {

    /**
     * Increment this row using a {@code RowUpdate} object.
     *
     * @param rowUpdate the {@code RowUpdate} object to apply to this row.
     */
    void inc(RowUpdate rowUpdate);

    /**
     * Serialize this row into a {@code ByteBuffer} object.
     *
     * @return a {@code ByteBuffer} object containing the serialized data.
     */
    ByteBuffer serialize();
}
