package org.petuum.app.forest;

import java.lang.Math;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import java.io.FileReader;
    
import java.io.FileWriter;

import org.petuum.jbosen.PsTableGroup;
import org.petuum.jbosen.row.float_.DenseFloatRow;
import org.petuum.jbosen.row.float_.DenseFloatRowUpdate;
import org.petuum.jbosen.row.int_.DenseIntRow;
import org.petuum.jbosen.row.int_.DenseIntRowUpdate;
import org.petuum.jbosen.table.IntTable;
import org.petuum.jbosen.table.FloatTable;
import org.petuum.jbosen.row.float_.FloatColumnIterator;
import org.petuum.jbosen.row.float_.FloatColumnMap;

public class ForestWorker implements Runnable {
    private int numFeatures;
    private int numSamples;
    private int numClasses;
    private int depth;
    private boolean convex;
    private DenseFloatRow vals;
    private DenseIntRow features;
    private DenseFloatRowUpdate valUpdates;
    private DenseIntRowUpdate featureUpdates;
    private String workerID;
    private String file;
    private String fileT;
    private FloatColumnMap[] data;
    private float[] labels;
    private IntTable featureTable;
    private FloatTable valTable;
    private FloatTable lossTable;
    private String modelOutput;  
    private int treesPerWorker;
    private int lossInterval;
    private String lossFileName;
    private DenseFloatRow[] valsCache;
    private DenseIntRow[] featuresCache;
    private boolean regression;    
    private boolean hdfs;
    private final int LEAF = -1;
    private int numWorkers;
    private int numData;
    private boolean featureOneBased;
    private int labelIndexFrom;
    private float subSample; // the subsampling number of examples to construct each tree
    private int featuresConsidered; // the numbers of feature to consider when looking for the best split at each node
    private FloatColumnMap[] dataT;
    private float[] labelsT;
    private int numDataT;

    
    public static class Config {
        public FloatTable valTable;
        public IntTable featureTable;
        public String file;
        public String fileT;
        public String workerID;
        public int numFeatures; 
        public int numClasses;
        public int treesPerWorker;
        public int depth;
        public float lambda;
        public float decayRate;
        public int lossInterval;
        public String output;  
        public String lossFileName;
        public Config(){}
        public boolean convex;
        public boolean regression;
        public boolean hdfs;
        public boolean featureOneBased;
        public int labelIndexFrom;
        public int numWorkers;
        public float subSample;
        public int featuresConsidered;
    }

    /*
    The class is used to contain a value of some feature in a data and its related label, the main purpose is for sorting.
    */
    public class BiStruct implements Comparable<BiStruct> {
        public float feature;
        public float label;
        public BiStruct(){
            feature = 0;
            label = 0;
        }
        public BiStruct(float f, float l){
            feature = f;
            label = l;
        }
        public int compareTo(BiStruct bo) {
            //BiStruct bo = (BiStruct) o;
            return this.feature < bo.feature ? -1 : this.feature > bo.feature ? 1 : 0; 
        }
        public void set(float f, float l){
            feature = f;
            label = l;
        }
    }
    
    public ForestWorker (Config config, FloatColumnMap[] pdata, float[] plabels, FloatColumnMap[] pdataT, float[] plabelsT) {
        treesPerWorker = config.treesPerWorker;
        workerID = config.workerID;
        numFeatures = config.numFeatures; 
        depth = config.depth;
        numClasses = config.numClasses;
        file = config.file;
        fileT = config.fileT;
        modelOutput = config.output;
        convex = config.convex;
        numWorkers = config.numWorkers;
        subSample = config.subSample;
        hdfs = config.hdfs;
        //getNumSamples();
        lossFileName = config.lossFileName;
        regression = config.regression;
        featureOneBased = config.featureOneBased;
        labelIndexFrom = config.labelIndexFrom;
        featuresConsidered = config.featuresConsidered;
        data = pdata;
        labels = plabels;
        dataT = pdataT;
        labelsT = plabelsT;
        numData = data.length;
        numSamples = Math.round(numData * subSample);
    }

    /*
    Update two rows in the two tables respectively.
    */
    private void update(int position) {
        featureTable.inc(position, featureUpdates);
        valTable.inc(position, valUpdates);
  
    }

    /*
    Partition the data at a node into left part and right part according to boundary.
    */
    private void partition(int best, float bestBoundary, FloatColumnMap[] data, float[] labels, FloatColumnMap[] dataL, float[] labelsL, FloatColumnMap[] dataR, float[] labelsR){
        int indexL = 0, indexR = 0;
        for (int i = 0; i < data.length; i++){
            if (data[i].get(best) < bestBoundary){
                dataL[indexL] = data[i];
                labelsL[indexL] = labels[i];
                indexL++;
            }
            else{
                dataR[indexR] = data[i];
                labelsR[indexR] = labels[i];
                indexR++;
            }
        }
    }

    /*
    Find the most popular value in an array for classification.
    */
    private float choice(float[] arr) {
        if (regression) return regressionChoice(arr);
        int[] hist = new int[numClasses];
        for (int i = 0; i < arr.length; i++) {
            hist[Math.round(arr[i])]++;
        }
        int best = hist[0];
        int index = 0; 
        for (int i  = 1; i < numClasses; i++) {
            if (hist[i] > best) {
                index = i;
                best = hist[i];
            }
        }
        return (float) index;
    }

    /*
    Calculate the mean of an array for regression.
    */
    private float regressionChoice(float[] arr) {
        float result = 0.0f;
        int count = 0;
        for (int i = 0; i < arr.length; i++) {
            result += arr[i];
            count ++;
        }
        if (count == 0) {
            return result;
        }
        return result/count;
    }

    /*
    Update the related position in the RowUpdate of a leaf.
    */
    private void generateLeaf(int position, FloatColumnMap[] data, float[] labels) {
        featureUpdates.set(position, LEAF);
        valUpdates.set(position, choice(labels));
    }
    
    /*
    Loss function for classification: information gain.
    */
    private float loss(BiStruct[] list, int[] LHist, int[] RHist, int lastPartition, int curPartition) {
        if (lastPartition == -1){
            for (int i = 0; i < list.length; i++) {
                BiStruct tmp = (BiStruct) list[i];
                RHist[Math.round(tmp.label)]++;
            }
        }
        // add the labels of (lastPartition, curPartition] to LHist and minus them from RHist
        for (int i = lastPartition + 1; i <= curPartition; i++){
            BiStruct tmp = (BiStruct) list[i];
            LHist[Math.round(tmp.label)]++;
            RHist[Math.round(tmp.label)]--;
        }
        int LTotal = curPartition + 1, RTotal = list.length - curPartition - 1;
        float result = 0.0f;
        for (int i = 0; i < LHist.length; i++) {
            if (LHist[i] != 0) {
                result -= (((float) LHist[i])/LTotal)*((float) Math.log(((double) LHist[i])/LTotal))*(((float) LTotal)/(LTotal + RTotal));
            }
        }
        for (int i = 0; i < RHist.length; i++) {
            if (RHist[i] != 0) {
                result -= (((float) RHist[i])/RTotal)*((float) Math.log(((double) RHist[i])/RTotal))*(((float) RTotal)/(LTotal + RTotal));
            }
        }
        return result;
        
    }
    
    /*
    Loss function for regression: sum of squared error.
    */
    private float regressionLoss(BiStruct[] list, float[] mean, int lastPartition, int curPartition) {
        if (lastPartition == -1){
            mean[0] = 0.0f;
            mean[1] = 0.0f;
            for (int i = 0; i < list.length; i++) {
                BiStruct tmp = (BiStruct) list[i];
                mean[1] += tmp.label;
            }
            mean[1] = mean[1] / list.length;
        }
        mean[0] = mean[0] * (lastPartition + 1);
        mean[1] = mean[1] * (list.length - lastPartition - 1);
        // add the labels of (lastPartition, curPartition] to mean[0] and minus them from mean[1]
        for (int i = lastPartition + 1; i <= curPartition; i++){
            BiStruct tmp = (BiStruct) list[i];
            mean[0] += tmp.label;
            mean[1] -= tmp.label;
        }
        mean[0] = mean[0] / (curPartition + 1);
        mean[1] = mean[1] / (list.length - curPartition - 1);

        float LLoss = 0.0f;
        float RLoss = 0.0f;
        for (int i = 0; i < list.length; i++) {
            BiStruct tmp = (BiStruct) list[i];
            if(i <= curPartition) {
                LLoss += (tmp.label-mean[0])*(tmp.label-mean[0]);
            }
            else {
                RLoss += (tmp.label-mean[1])*(tmp.label-mean[1]);
            }
        }
        return LLoss + RLoss;
        
    }

    /*
    Find the minimized loss and the related boundary for a given sorted list.
    */
    private float[] boundary(BiStruct[] list) {
        float min_loss = Float.POSITIVE_INFINITY; // the minimized loss
        float current_loss = 0.0f;
        float result = 0.0f; // the boundary that minimize the loss function
        int lNum = 0; // (lNum + 1) equals the number of the data splitted to the left child node

        int[] LHist = new int[numClasses];
        int[] RHist = new int[numClasses];
        float[] mean = new float[2]; // mean of left part and right part

        int lastPartition = -1; // record the last index of partition
        for (int i = 0; i < list.length-1; i++) {
            // find the index in the list that value of the index doesn't equal value of (index+1)
            while ((i < list.length - 1) && (list[i].feature == list[i+1].feature))
                i++;
            if (i == list.length - 1)
                break;
            float mid = (list[i].feature + list[i+1].feature)/2;

            // (lastPartition, i] of the list should be splitted to the left child node
            if (regression) {
                current_loss = regressionLoss(list, mean, lastPartition, i);
            }
            else{
                current_loss = loss(list, LHist, RHist, lastPartition, i);
            }
            lastPartition = i;
            if (convex && current_loss > min_loss)
            {
                float[] rVal = new float[3];
                rVal[0] = result;
                rVal[1] = min_loss;
                rVal[2] = (float) lNum;
                return rVal;
            }
            if (current_loss <= min_loss) {
                result = mid;
                min_loss = current_loss;
                lNum = i + 1;
            }
        }
        float[] rVal = new float[3];
        rVal[0] = result;
        rVal[1] = min_loss;
        rVal[2] = (float) lNum;
        return rVal;
    }

    /*
    Main function to construct a tree.
    */
    public void generateTree(int position, FloatColumnMap[] data, float[] labels) {
        //System.out.println( (int) Math.round(Math.pow(2, depth)-1)); 
        if(position >= (int) Math.round(Math.pow(2, depth)-1)) {
            generateLeaf(position, data, labels);
            return;
        }
        //System.out.println("Splitting node " + Integer.toString(position));
        float minSplitCosts = Float.POSITIVE_INFINITY;
        int best = -1;
        float bestBoundary = 0;
        int lNum = 0;
        // randomly generate a set of features to consider when looking for the best split at a node
        int[] featuresConsideredList = randomFeatures();
        BiStruct[] list = new BiStruct[data.length];
        for (int i = 0; i < featuresConsideredList.length; i++) {
            for (int j = 0; j < data.length; j++){
                if (i == 0){
                    list[j] = new BiStruct(data[j].get(featuresConsideredList[i]), labels[j]);
                }
                else{
                    list[j].set(data[j].get(featuresConsideredList[i]), labels[j]);
                }
            }
            Arrays.sort(list);
            float[] rVal = boundary(list);
            if (minSplitCosts > rVal[1]) {
                 best = featuresConsideredList[i];
                 bestBoundary = rVal[0];
                 minSplitCosts = rVal[1];
                 lNum = (int) rVal[2];
            }
            //System.out.println(currentCost);
        }
        if(best != -1) {
            // update the related index of the Updates
            featureUpdates.set(position, best+1); 
            valUpdates.set(position, bestBoundary);
            // partition data at current node into left part and right part
            FloatColumnMap[] dataL = new FloatColumnMap[lNum];
            float[] labelsL = new float[lNum];
            FloatColumnMap[] dataR = new FloatColumnMap[data.length - lNum];
            float[] labelsR = new float[data.length - lNum];
            partition(best, bestBoundary, data, labels, dataL, labelsL, dataR, labelsR);
            // recursively generate the tree
            generateTree(position * 2 + 1, dataL, labelsL);
            generateTree(position * 2 + 2, dataR, labelsR);
        }
        else {
            generateLeaf(position, data, labels);
        }
            
    }

    /*
    Auxiliary function for output information.
    */
    private String treeToString(DenseFloatRow values, DenseIntRow features) {
        String result = "";
        result += features.get(0);
        for (int i = 1; i < features.capacity(); i++) {
            result += ", " + features.get(i);
        }
        result += "\n" + values.get(0);
        for (int i = 1; i < values.capacity(); i++) {
            result += ", " + values.get(i);
        }
        return result + "\n";
    }

    /*
    Calculate accuracy of classification.
    */
    private float accuracy(DenseFloatRow[] valuesArr, DenseIntRow featuresArr[], FloatColumnMap[] data, float[] labels) {
        if (regression) return regressionAccuracy(valuesArr, featuresArr, data, labels);
        int correct = 0;
        for (int i = 0; i < data.length; i++) {
            if (Math.round(evaluateForest(valuesArr, featuresArr, data[i])) == Math.round(labels[i])) {
                correct++;
            }
        }
        return ((float) correct)/data.length;
    }
    
    /*
    Regression accuracy: mean of squared error.
    */
    private float regressionAccuracy(DenseFloatRow[] valuesArr, DenseIntRow featuresArr[], FloatColumnMap[] data, float[] labels) {
        float Loss = 0.0f;
        float prediction;
        for (int i = 0; i < labels.length; i++) {
            prediction = evaluateForest(valuesArr, featuresArr, data[i]);
            Loss += (labels[i]-prediction)*(labels[i]-prediction);
        }
        return Loss/data.length;
    }

    /*
    Go through all the trees to evaluate the forest.
    */
    private float evaluateForest(DenseFloatRow[] valuesArr, DenseIntRow featuresArr[], FloatColumnMap data) {
        float[] results = new float[valuesArr.length];
        boolean[] filter = new boolean[valuesArr.length];
        int numR = 0;
        for (int i = 0; i < results.length; i++) {
            if (featuresArr[i].get(0) != 0) {
                filter[i] = true;
                numR++;
                results[i] = evaluateTree(0, valuesArr[i], featuresArr[i], data);
            }
            else filter[i] = false;
        }
        float[] filteredResult = new float[numR];
        int indexR = 0;
        for (int i = 0; i < results.length; i++){
            if (filter[i]){
                filteredResult[indexR] = results[i];
                indexR++;
            }
        }
      //  System.out.println(Arrays.toString(results));
        return choice(filteredResult);
       
    }

    /*
    Go through all the nodes to arrive at a leaf of the tree and return the value of the leaf.
    */
    private float evaluateTree(int position, DenseFloatRow values, DenseIntRow features, FloatColumnMap data) {
        if (features.get(position) == LEAF) return values.get(position);
        int feature = features.get(position) - 1;
        if(values.get(position) <= data.get(feature))
            return evaluateTree(position * 2 + 2, values, features, data);
        return evaluateTree(position * 2 + 1, values, features, data);
    }

    /*
    Write a string to file.
    */
    private boolean writeToFile(String filename, String content) {
        try {
            BufferedFileWriter f = new BufferedFileWriter(filename, hdfs, false);
            f.write(content);
            f.close();
        }
        catch (Exception e) {return false;}
        return true; 
    }

    /*
    Bootstrap sampling to get a subset of whole data.
    */
    private void randomSubset(FloatColumnMap[] randomData, float[] randomLabels) {
        java.util.Random r=new java.util.Random(); 
        for(int i=0;i<numSamples;i++){ 
            int index = (r.nextInt() % numData + numData) % numData;
            randomData[i] = data[index];
            randomLabels[i] = labels[index];
        } 
    }        

    /*
    Randomly select a subset of features to consider at a node.
    */
    private int[] randomFeatures(){
        assert featuresConsidered <= numFeatures;
        int[] result = new int[featuresConsidered];
        boolean[] filter = new boolean[numFeatures];
        for (int i = 0; i < featuresConsidered; i++)
            filter[i] = true;
        Util.shuffle(filter);
        int index = 0;
        for (int i = 0; i < numFeatures; i++){
            if (filter[i]){
                result[index] = i;
                index++;
            }
        }
        return result;
    }

    public void run() {
        PsTableGroup.registerWorkerThread();
        PsTableGroup.globalBarrier();
        featureTable = PsTableGroup.getIntTable(0);        
        valTable = PsTableGroup.getFloatTable(1);          
        lossTable = PsTableGroup.getFloatTable(2);          
        boolean[] subset;
        PsTableGroup.clock(); 
        int totalWorkers = PsTableGroup.getNumClients() * PsTableGroup.getNumLocalWorkerThreads();
        int treeCapacity = (int) Math.round(Math.pow(2, depth+1)) -1; 
        long initTimeBegin = System.currentTimeMillis();
        for (int i = Integer.parseInt(workerID); i < totalWorkers * treesPerWorker; i+= totalWorkers) {
            featureUpdates = new DenseIntRowUpdate(treeCapacity); 
            valUpdates = new DenseFloatRowUpdate(treeCapacity); 
            FloatColumnMap[] randomData = new FloatColumnMap[numSamples];
            float[] randomLabels = new float[numSamples];
            randomSubset(randomData, randomLabels);
            generateTree(0, randomData, randomLabels);
            update(i);
            System.out.println("Tree #" + Integer.toString(i) + "/" + Integer.toString(totalWorkers * treesPerWorker) + " complete");
        }
        PsTableGroup.clock();  
        String model = "";   
        PsTableGroup.globalBarrier();
        if (workerID.equals("0")) {
            long initTimeElapsed = System.currentTimeMillis() - initTimeBegin;
            System.out.println("Training in " + initTimeElapsed / 1000.0 + " s");
        }

        initTimeBegin = System.currentTimeMillis();
        valsCache = new DenseFloatRow[PsTableGroup.getNumClients() * PsTableGroup.getNumLocalWorkerThreads() * treesPerWorker];
        featuresCache = new DenseIntRow[PsTableGroup.getNumClients() * PsTableGroup.getNumLocalWorkerThreads() * treesPerWorker];
        for (int i = 0; i < PsTableGroup.getNumClients() * PsTableGroup.getNumLocalWorkerThreads() * treesPerWorker; i++) {
            vals = valTable.get(i);
            features = featureTable.get(i);
            if (!modelOutput.equals(""))
                model += "\n"+treeToString(vals, features); 
            valsCache[i] = vals;
            featuresCache[i] = features;
        } 
        if (workerID.equals("0")) {
            long initTimeElapsed = System.currentTimeMillis() - initTimeBegin;
            System.out.println("Downloading all the parameters from PS in " + initTimeElapsed / 1000.0 + " s");
        }

        initTimeBegin = System.currentTimeMillis();
        
        DenseFloatRowUpdate lossUpdates = new DenseFloatRowUpdate(1);
        if (fileT.equals("")){
            lossUpdates.set(0, accuracy(valsCache, featuresCache, data, labels)/totalWorkers); 
        }
        else{
            try {
                int numDataT = dataT.length;
                int base = numDataT / numWorkers * Integer.parseInt(workerID);
                int nums = Integer.parseInt(workerID) == numWorkers - 1 ? numDataT - base : numDataT / numWorkers;
                FloatColumnMap[] dataTPart = new FloatColumnMap[nums];
                float[] labelsTPart = new float[nums];
                for (int i = 0; i < nums; i++){
                    dataTPart[i] = dataT[base + i];
                    labelsTPart[i] = labelsT[base + i];
                }
                lossUpdates.set(0, accuracy(valsCache, featuresCache, dataTPart, labelsTPart)/totalWorkers);
                /*for output the predicting score of the testing set
                if (workerID.equals("0")){
                    FileWriter fw = new FileWriter("predict_labels");
                    for (int i = 0; i < dataT.length; i++) {
                       float tmp = 0;
                       float cnt = 0;
                       for (int j = 0; j < valsCache.length; j++){
                          if (featuresCache[j].get(0) != 0){
                            tmp += evaluateTree(0, valsCache[j], featuresCache[j], dataT[i]);
                            cnt += 1.0f;
                          }
                        }
                        tmp = tmp / cnt;
                        fw.write(Float.toString(tmp) + " " + Integer.toString((int) labelsT[i]) + "\n");
                    }
                    fw.close();
                }*/
            }
            catch (Exception e) {}
        }
        lossTable.inc(0, lossUpdates);
        PsTableGroup.clock();  
        PsTableGroup.globalBarrier();

        if (workerID.equals("0")) {
            long initTimeElapsed = System.currentTimeMillis() - initTimeBegin;
            System.out.println("Testing in " + initTimeElapsed / 1000.0 + " s");

            if (!modelOutput.equals(""))
                writeToFile(modelOutput, model);
            if (fileT.equals("")){
                if (!regression){
                    writeToFile(lossFileName, "Training accuracy: " + lossTable.get(0,0) + "\n"); 
                    System.out.println("Training accuracy: " + lossTable.get(0,0));
                }
                else{
                    writeToFile(lossFileName, "Training loss(MSE): " + lossTable.get(0,0) + "\n"); 
                    System.out.println("Training loss(MSE): " + lossTable.get(0,0));   
                }
            }
            else{
                if (!regression){
                    writeToFile(lossFileName, "Testing accuracy: " + lossTable.get(0,0) + "\n"); 
                    System.out.println("Testing accuracy: " + lossTable.get(0,0));
                }
                else{
                    writeToFile(lossFileName, "Testing loss(MSE): " + lossTable.get(0,0) + "\n"); 
                    System.out.println("Testing loss(MSE): " + lossTable.get(0,0));
                }
            }
        }

        PsTableGroup.deregisterWorkerThread();
    }
}
