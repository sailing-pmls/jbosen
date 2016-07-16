package org.petuum.app.forest;

import org.petuum.jbosen.row.float_.SparseFloatRow;
import org.petuum.jbosen.row.float_.SparseFloatRowUpdate;
import org.petuum.jbosen.row.float_.FloatColumnIterator;

import java.util.Random;


public class Util {
    private static final Double kCutOff = 1e-15;
    
    public static float dotProd(SparseFloatRowUpdate vec1, SparseFloatRow vec2) {
        FloatColumnIterator it = vec1.iterator();
        float result = 0.0f;
        while (it.hasNext()) {
            it.advance();
            int index = it.getColumnId();
            result += vec2.get(index) * it.getValue();
        }
        //for (int i = 0; i<len; i++) {
        //    result += vec1[i] * vec2[i];
        // }
        return result;
    }

    public static boolean exists(boolean[] arr) {
        for (int i = 0; i < arr.length; i++) {
            if(arr[i]) return true;
        }
        return false;
    }

    public static void shuffle(boolean[] arr) {
        Random rand = new Random();
        int pos; 
        boolean temp;
        for (int i = arr.length-1; i>0; i--) {
            pos = rand.nextInt(i+1);
            temp = arr[pos];
            arr[pos] = arr[i];
            arr[i] = temp;
        }
    } 
    
    public static int argmax(float[] args) {
        int result = 0;
        for (int i = 1; i < args.length; i++) {
            if (Float.compare(args[result],args[i]) < 0) result = i;
        }
        return result;
    }
    
    public static double safeLog(double x) {
        if (Math.abs(x) < kCutOff) {
            x = kCutOff;
        }
        return Math.log(x);
    }
    
    public static void softMax(float[] yVec) {
        assert yVec != null;

        for (int i = 0; i < yVec.length; i++) {
            double v = yVec[i];
            if (Math.abs(v) < kCutOff) {
                yVec[i] = kCutOff.floatValue();
            }
        }

        double lsum = logSumVec(yVec);
        for (int i = 0; i < yVec.length; i++) {
            yVec[i] = fastExp(yVec[i] - lsum);
            if (yVec[i] > 1) {
                yVec[i] = 1f;
            }
        }
    }

    private static float fastExp(double d) {
        return new Double(Math.exp(d)).floatValue();
    }

    private static double logSumVec(float[] yVec) {
        float sum = 0;
        sum = yVec[0];
        for (int i = 1; i < yVec.length; i++) {
            sum = logSum(sum, yVec[i]);
        }
        return sum;
    }

    private static float logSum(float logA, float logB) {
        if (logA < logB) {
            return logB + fastLog(1 + fastExp(logA - logB));
        } else {
            return logA + fastLog(1 + fastExp(logB - logA));

        }
    }

    private static float fastLog(float f) {
        double v = f;
        float logV = new Double(safeLog(v)).floatValue();
        return logV;
    }

}
