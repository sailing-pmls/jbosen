package org.petuum.app.forest;

/**
 * 
 * @author yihuaf
 *
 */
import java.lang.Math;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.petuum.jbosen.row.float_.DenseFloatRow;
import org.petuum.jbosen.row.float_.DenseFloatRowUpdate;
import org.petuum.jbosen.row.float_.FloatRow;
import org.petuum.jbosen.row.float_.FloatColumnMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ForestDataLoading {

    private static final Logger logger = LoggerFactory
            .getLogger(ForestDataLoading.class);

    /*
    Parse data in a LibSVM format file into arrays: trainFeatures and trainLabels.
    */
    public static void readDataLabelLibSVM(String filename, int featureDim,
            int numData, FloatColumnMap[] trainFeatures, float[] trainLabels,
            boolean featureOneBased, int labelIndexFrom, boolean regression, boolean hdfs) {

        assert trainFeatures.length == numData;
        assert trainLabels.length == numData;

        BufferedFileReader input = null;
        try {
            input = new BufferedFileReader(filename, hdfs);
            int i = 0;
            String line = null;
            while ((line = input.readLine()) != null && i < numData) {
                float label = parseLibSVMLine(line, trainFeatures[i],
                        featureOneBased, labelIndexFrom, regression);
                trainLabels[i] = label;
                i++;
            }
            assert (numData == i);
        } catch (IOException e) {
            logger.error("failed to read the file " + filename);
            System.exit(-1);
        } finally {
            try {
                input.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return;
    }

    /*
    Parse a line in the LibSVM format data file, this file is for classification.
    */
    private static float parseLibSVMLine(String line, 
            FloatColumnMap featureVals, boolean featureOneBased, int labelIndexFrom, boolean regression) {
        if (regression) return parseRegressionLibSVMLine(line, featureVals, featureOneBased, labelIndexFrom);

        Pattern pattern = Pattern.compile("^([0-9]+)\\s*(.+)$");
        Matcher matcher = pattern.matcher(line);

        if (!matcher.matches()) {
            logger.error("Failed to match the libsvm line: " + line);
            System.exit(-1);
        }
        // Read label
        float label = Float.parseFloat(matcher.group(1));

        // Minus the starting index of the index, label is based on zero
        label -= labelIndexFrom;

        // Read feature pairs
        String[] featurePairs = matcher.group(2).split("\\s");

        int i = 0;
        for (String pair : featurePairs) {
            String[] splitedPair = pair.split(":");
            assert splitedPair.length == 2;
            int featureId = Integer.parseInt(splitedPair[0]);
            // If index of feature starts from 1, then featureId minus 1 so that it is based on zero
            if (featureOneBased) {
                featureId--;
            }

            float featureVal = Float.parseFloat(splitedPair[1]);
            featureVals.inc(featureId,featureVal);
            i++;
        }

        return label;
    }
    
    /*
    Parse a line in the LibSVM format data file, this file is for regression.
    */
    private static float parseRegressionLibSVMLine(String line, 
            FloatColumnMap featureVals, boolean featureOneBased, int labelIndexFrom) {

        String[] featurePairs = line.split("\\s") ;
        float label = Float.parseFloat(featurePairs[0]);
        featurePairs = Arrays.copyOfRange(featurePairs, 1, featurePairs.length);

        int i = 0;
        for (String pair : featurePairs) {
            String[] splitedPair = pair.split(":");
            assert splitedPair.length == 2;
            int featureId = Integer.parseInt(splitedPair[0]);
            if (featureOneBased) {
                featureId--;
            }

            float featureVal = Float.parseFloat(splitedPair[1]);
            featureVals.inc(featureId,featureVal);
            i++;
        }

        return label;
    }

}
