package org.petuum.jbosen.row;

import java.nio.ByteBuffer;

/**
 * This interface specifies a factory for row updates. An implementation of
 * this interface must be thread-safe.
 */
public interface RowUpdateFactory {

    /**
     * Create and return a new row update.
     *
     * @return new row update constructed by this factory.
     */
    RowUpdate createRowUpdate();

    /**
     * Create a new row update from serialized data.
     *
     * @param buffer buffer containing serialized data for a row update.
     * @return new row update constructed from the serialized data.
     */
    RowUpdate deserializeRowUpdate(ByteBuffer buffer);
}
