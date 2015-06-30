package org.petuum.app.matrixfact;

import org.petuum.jbosen.table.DoubleTable;
import org.petuum.jbosen.PsTableGroup;
import org.petuum.jbosen.row.double_.DoubleRow;

import java.util.ArrayList;
import java.text.DecimalFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LossRecorder {
    private static final Logger logger =
        LoggerFactory.getLogger(LossRecorder.class);

    public static final int kMaxNumFields = 100; // Track at most 100 fields.
    public static final int kCacheSize = 1000;
    public static final int kLossTableId = 999;

    private DoubleTable lossTable;
    private int numFields = 0;
    // The index of a field is its column idx in lossTable.
    private ArrayList<String> fields = new ArrayList<String>();
    private int maxEval = 0;

    // This needs to be called before PsTableGroup.createTableDone().
    public static void createLossTable() {
        PsTableGroup.createDenseDoubleTable(kLossTableId, 0, kMaxNumFields);
    }

    public LossRecorder() {
        lossTable = null;
    }

    // All workers need to register fields in the same order.
    public void registerField(String fieldname) {
        fields.add(fieldname);
        numFields++;
    }

    // Return -1 if not found.
    private int findField(String fieldname) {
        for (int j = 0; j < numFields; ++j) {
            if (fields.get(j).equals(fieldname)) {
                return j;
            }
        }
        return -1;
    }

    public void incLoss(int ith, String fieldname, double val) {
        if (lossTable == null) {
            lossTable = PsTableGroup.getDoubleTable(kLossTableId);
        }
        int fieldIdx = findField(fieldname);
        assert fieldIdx != -1;
        maxEval = Math.max(ith, maxEval);
        lossTable.inc(ith, fieldIdx, val);
    }

    public String printAllLoss() {
        if (lossTable == null) {
            lossTable = PsTableGroup.getDoubleTable(kLossTableId);
        }
        // Print header.
        String header = "";
        for (int j = 0; j < numFields; ++j) {
            header += fields.get(j) + " ";
        }
        header += "\n";

        // Print each row.
        DecimalFormat doubleFormat = new DecimalFormat("#.00");
        DecimalFormat intFormat = new DecimalFormat("#");
        String stats = "";
        for (int i = 0; i <= maxEval; ++i) {
            DoubleRow lossRow = lossTable.get(i);
            for (int j = 0; j < numFields; ++j) {
                double val = lossRow.get(j);
                String formatVal = (val % 1 == 0) ? intFormat.format(val) :
                    doubleFormat.format(val);
                stats += formatVal + " ";
                assert !Double.isNaN(val);
            }
            stats += "\n";
        }
        return header + stats;
    }

    public String printOneLoss(int ith) {
        if (lossTable == null) {
            lossTable = PsTableGroup.getDoubleTable(kLossTableId);
        }
        DoubleRow lossRow = lossTable.get(ith);
        String stats = "";
        DecimalFormat doubleFormat = new DecimalFormat("#.00");
        DecimalFormat intFormat = new DecimalFormat("#");
        for (int j = 0; j < numFields; ++j) {
            double val = lossRow.get(j);
            String formatVal = (val % 1 == 0) ? intFormat.format(val) :
                doubleFormat.format(val);
            stats += fields.get(j) + ": " + formatVal + " ";
            assert !Double.isNaN(val);
        }
        return stats;
    }
}
