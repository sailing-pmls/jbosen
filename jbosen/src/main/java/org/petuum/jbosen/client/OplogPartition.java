package org.petuum.jbosen.client;

import gnu.trove.set.TIntSet;
import org.petuum.jbosen.row.RowUpdate;

interface OplogPartition {
    RowUpdate createRowUpdate(int rowId);

    RowUpdate getRowUpdate(int rowId);

    RowUpdate removeRowUpdate(int rowId);

    TIntSet getRowIds();
}
