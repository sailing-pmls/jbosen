package org.petuum.app.matrixfact;

import org.petuum.jbosen.table.DoubleTable;
import org.petuum.jbosen.row.double_.DenseDoubleRowUpdate;
import org.petuum.jbosen.row.double_.DoubleRow;
import org.petuum.jbosen.row.double_.DoubleRowUpdate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class MatrixFactCore {
    private static final Logger logger =
        LoggerFactory.getLogger(MatrixFactCore.class);

    // Perform a single SGD on a rating and update LTable and RTable
    // accordingly.
    public static void sgdOneRating(Rating r, double learningRate,
            DoubleTable LTable, DoubleTable RTable, int K, double lambda) {
        int i = r.userId;
        int j = r.prodId;
        // Cache a row for DenseRow bulk-read to avoid locking at each read
        // of entry.
        DoubleRow LiCache = LTable.get(i);
        DoubleRow RjCache = RTable.get(j);

        double pred = 0;   // <Li, Rj> dot product is the predicted val
        for (int k = 0; k < K; ++k) {
            pred += LiCache.get(k) * RjCache.get(k);
            assert !Double.isNaN(pred) : "i: " + i + " j: " + j + " k: " + k;
        }

        // Now update L(i,:) and R(:,j) based on the loss function at
        // X(i,j).
        // The loss function at X(i,j) is ( X(i,j) - L(i,:)*R(:,j) )^2.
        //
        // The gradient w.r.t. L(i,k) is -2*X(i,j)R(k,j) +
        // 2*L(i,:)*R(:,j)*R(k,j).
        // The gradient w.r.t. R(k,j) is -2*X(i,j)L(i,k) +
        // 2*L(i,:)*R(:,j)*L(i,k).
        DoubleRowUpdate LiUpdate = new DenseDoubleRowUpdate(K);
        DoubleRowUpdate RjUpdate = new DenseDoubleRowUpdate(K);
        double grad_coeff = -2 * (r.rating - pred);
        double nnzRowi = LiCache.get(K);
        double nnzColj = RjCache.get(K);
        for (int k = 0; k < K; ++k) {
            // Compute update for L(i,k)
            double LikGradient = grad_coeff * RjCache.get(k)
                + 2 * lambda / nnzRowi * LiCache.get(k);
            LiUpdate.set(k, -LikGradient * learningRate);
            // Compute update for R(k,j)
            double RkjGradient = grad_coeff * LiCache.get(k)
                + 2 * lambda / nnzColj * RjCache.get(k);
            RjUpdate.set(k, -RkjGradient * learningRate);
        }
        LTable.inc(i, LiUpdate);
        RTable.inc(j, RjUpdate);
    }

    // Evaluate square loss on entries [elemBegin, elemEnd), and L2-loss on of
    // row [LRowBegin, LRowEnd) of LTable,  [RRowBegin, RRowEnd) of Rtable.
    // Note the interval does not include LRowEnd and RRowEnd. Record the loss to
    // lossRecorder.
    public static void evaluateLoss(ArrayList<Rating> ratings, int ithEval,
            int elemBegin, int elemEnd, DoubleTable LTable,
            DoubleTable RTable, int LRowBegin, int LRowEnd, int RRowBegin,
            int RRowEnd, LossRecorder lossRecorder, int K, double lambda) {
        double sqLoss = 0f;
        DoubleRow LiCache;
        DoubleRow RjCache;
        for (int ratingId = elemBegin; ratingId < elemEnd; ratingId++) {
            Rating r = ratings.get(ratingId);
            LiCache = LTable.get(r.userId);
            RjCache = RTable.get(r.prodId);
            double pred = 0f;   // <Li, Rj> dot product is the predicted val
            for (int k = 0; k < K; ++k) {
                pred += LiCache.get(k) * RjCache.get(k);
            }
            sqLoss += (r.rating - pred) * (r.rating - pred);
            assert !Double.isNaN(sqLoss);
        }
        lossRecorder.incLoss(ithEval, "SquareLoss", sqLoss);

        // Eval L2 reg loss on rows/cols assigned to this worker.
        DoubleRow rowCache;
        double l2Loss = 0f;
        for (int i = LRowBegin; i < LRowEnd; i++) {
            rowCache = LTable.get(i);
            for (int k = 0; k < K; ++k) {
                l2Loss += rowCache.get(k) * rowCache.get(k);
                assert !Double.isNaN(l2Loss);
            }
        }
        for (int i = RRowBegin; i < RRowEnd; i++) {
            rowCache = RTable.get(i);
            for (int k = 0; k < K; ++k) {
                l2Loss += rowCache.get(k) * rowCache.get(k);
                assert !Double.isNaN(l2Loss);
            }
        }
        double totalLoss = sqLoss + lambda * l2Loss;
        assert !Double.isNaN(totalLoss);
        lossRecorder.incLoss(ithEval, "FullLoss", totalLoss);
        lossRecorder.incLoss(ithEval, "NumSamples", elemEnd - elemBegin);
    }
}
