package org.petuum.app.forest;

import org.petuum.jbosen.PsConfig;
import org.petuum.jbosen.PsTableGroup;

import java.lang.Thread;
import java.lang.Integer;
import java.lang.Float;
import org.petuum.jbosen.row.float_.FloatColumnMap;
import org.petuum.jbosen.row.float_.DenseFloatRowUpdate;
import org.petuum.jbosen.row.float_.SparseFloatRowUpdate;
import java.io.LineNumberReader;



public class Forest {

    /*
    Get the samples of the whole training set.
    */
    private static int getNumSamples(String file, boolean hdfs) {
        try {
            BufferedFileReader fr = new BufferedFileReader(file, hdfs);
            LineNumberReader lr = new LineNumberReader(fr.getReader());
            lr.skip(Long.MAX_VALUE);
            int numData = lr.getLineNumber();
            fr.close();
            return numData;
        }
        catch (Exception e) {}
        return 0;
    }
    
    public static void main(String[] args) {
        CmdArgs mc = new CmdArgs(args);
        String clientID = Integer.toString(mc.clientId);
        //Initialize TableGroup and then create tables
        PsTableGroup.init(mc); 
        PsTableGroup.createDenseIntTable(0, mc.staleness, (int) Math.round(Math.pow(2, mc.depth+1))-1);
        PsTableGroup.createDenseFloatTable(1, mc.staleness, (int) Math.round(Math.pow(2, mc.depth+1))-1);
        PsTableGroup.createDenseFloatTable(2, mc.staleness, 1);
        
            
        try { Thread.sleep(1000);}
        catch (InterruptedException e) {}
        System.out.print("NUM CLIENTS: ");
        System.out.println(PsTableGroup.getNumClients());
 
        int numWorkerThreads = mc.numLocalWorkerThreads;
        Thread[] workers = new Thread[numWorkerThreads];

        System.out.println("Start loading training set.");
        int numData = getNumSamples(mc.dataFile, mc.hdfs);
        FloatColumnMap[] data;
        float[] labels;
        if (mc.sparse){
            data = new SparseFloatRowUpdate[numData];
            for (int i = 0; i < numData; i++) {
                data[i] = new SparseFloatRowUpdate();
            }
        }
        else{
            data = new DenseFloatRowUpdate[numData];
            for (int i = 0; i < numData; i++) {
                data[i] = new DenseFloatRowUpdate(mc.numFeatures);
            }
        }
        labels = new float[numData];
        ForestDataLoading.readDataLabelLibSVM(mc.dataFile, mc.numFeatures, numData, data, labels, mc.featureOneBased, mc.labelIndexFrom, mc.regression, mc.hdfs);
        System.out.println("Finish loading training set.");

        FloatColumnMap[] dataT = null;
        float[] labelsT = null;
        if (!mc.dataFileT.equals("")){
            System.out.println("Start loading testing set.");
            int numDataT = getNumSamples(mc.dataFileT, mc.hdfs);
            if (mc.sparse){
                dataT = new SparseFloatRowUpdate[numDataT];
                for (int i = 0; i < numDataT; i++) {
                    dataT[i] = new SparseFloatRowUpdate();
                }
            }
            else{
                dataT = new DenseFloatRowUpdate[numDataT];
                for (int i = 0; i < numDataT; i++) {
                    dataT[i] = new DenseFloatRowUpdate(mc.numFeatures);
                }
            }
            labelsT = new float[numDataT];
            ForestDataLoading.readDataLabelLibSVM(mc.dataFileT, mc.numFeatures, numDataT, dataT, labelsT, mc.featureOneBased, mc.labelIndexFrom, mc.regression, mc.hdfs);
            System.out.println("Finish loading testing set.");
        }
        
        //Initialize workers
        for (int i = 0; i < numWorkerThreads; i++) {
            ForestWorker.Config config = new ForestWorker.Config();
            config.file = mc.dataFile;
            config.fileT = mc.dataFileT;
            config.numFeatures = mc.numFeatures;
            config.numClasses = mc.numClasses;
            config.numWorkers = PsTableGroup.getNumClients() * numWorkerThreads;
            config.treesPerWorker = mc.numTrees / config.numWorkers;
            config.depth = mc.depth;
            config.lossFileName = mc.lossFile;
            config.workerID = Integer.toString(Integer.parseInt(clientID)*numWorkerThreads+i);            
            config.output = mc.outputFile;
            config.regression = mc.regression;
            config.convex = false; 
            config.hdfs = mc.hdfs;
            config.featureOneBased = mc.featureOneBased;
            config.labelIndexFrom = mc.labelIndexFrom;
            config.subSample = mc.subSample;
            //config.sparse = mc.sparse;
            switch(mc.maxFeatures){
                case "sqrt":
                    config.featuresConsidered = (int) Math.round(Math.sqrt((double)mc.numFeatures));
                    break;
                case "all":
                    config.featuresConsidered = mc.numFeatures;
                    break;
                case "log2":
                    config.featuresConsidered = (int) Math.round(Math.log((double)mc.numFeatures / Math.log(2.0)));
                default:
                    try{
                        config.featuresConsidered = Integer.parseInt(mc.maxFeatures);
                    }
                    catch(NumberFormatException e){
                        try{
                            config.featuresConsidered = Math.round(Float.parseFloat(mc.maxFeatures) * mc.numFeatures);
                        }
                        catch(NumberFormatException e1){
                            config.featuresConsidered = mc.numFeatures;
                        }
                    }
            }
            workers[i] = new Thread(new ForestWorker(config, data, labels, dataT, labelsT));
        }
        
        //run workers concurrently
        for (int i = 0; i < numWorkerThreads; i++) {
            workers[i].start();
        } 
        
        //wait for workers to finish
        for (Thread t : workers) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        PsTableGroup.shutdown();
    }
}

