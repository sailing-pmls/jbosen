package org.petuum.jbosen.common;

import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import org.petuum.jbosen.common.util.HostInfo;

import java.util.ArrayList;

public class GlobalContext {
    public static final int MAX_NUM_THREADS_PER_CLIENT = 1000;
    public static final int BG_THREAD_ID_START_OFFSET = 100;
    public static final int WORKER_THREAD_ID_OFFSET = 200;
    public static final int SERVER_THREAD_ID_START_OFFSET = 1;

    private static int numClients;
    private static int numLocalCommChannels;
    private static int numLocalWorkerThreads;

    private static TIntObjectMap<HostInfo> serverMap;
    private static HostInfo nameNodeHostInfo;
    private static TIntList serverIds;

    private static int clientId;
    private static int localIdMin;

    public static int getThreadIdMin(int clientId) {
        return clientId * MAX_NUM_THREADS_PER_CLIENT;
    }

    public static int getThreadIdMax(int clientId) {
        return (clientId + 1) * MAX_NUM_THREADS_PER_CLIENT - 1;
    }

    public static int getNameNodeId() {
        return 0;
    }

    public static int getNameNodeClientId() {
        return 0;
    }

    public static boolean amINameNodeClient() {
        return clientId == getNameNodeClientId();
    }

    public static int getBgThreadId(int clientId, int commChannelIdx) {
        return getThreadIdMin(clientId) + BG_THREAD_ID_START_OFFSET
                + commChannelIdx;
    }

    public static int getHeadBgId(int clientId) {
        return getBgThreadId(clientId, 0);
    }

    public static int getServerThreadId(int clientId, int commChannelIdx) {
        return getThreadIdMin(clientId) + SERVER_THREAD_ID_START_OFFSET
                + commChannelIdx;
    }

    public static ArrayList<Integer> getServerThreadIDs(int commChannelIdx) {
        ArrayList<Integer> serverThreadIds = new ArrayList<>();
        for (int i = 0; i < GlobalContext.numClients; i++) {
            serverThreadIds.add(getServerThreadId(i, commChannelIdx));
        }
        return serverThreadIds;
    }

    public static int threadIdToClientId(int threadId) {
        return threadId / MAX_NUM_THREADS_PER_CLIENT;
    }

    public static void init(int numLocalCommChannels, int numLocalWorkerThreads, TIntObjectMap<HostInfo> hostMap, int clientId) {

        GlobalContext.numLocalCommChannels = numLocalCommChannels;
        GlobalContext.numLocalWorkerThreads = numLocalWorkerThreads;
        GlobalContext.numClients = hostMap.size();
        GlobalContext.clientId = clientId;
        GlobalContext.localIdMin = getThreadIdMin(clientId);
        GlobalContext.serverMap = new TIntObjectHashMap<>();
        GlobalContext.serverIds = new TIntArrayList();

        for (TIntObjectIterator<HostInfo> entry = hostMap.iterator(); entry
                .hasNext(); ) {
            entry.advance();
            HostInfo hostInfo = entry.value();
            int portNum = Integer.parseInt(hostInfo.port);

            if (entry.key() == getNameNodeId()) {
                GlobalContext.nameNodeHostInfo = hostInfo;

                portNum++;
                hostInfo = new HostInfo(hostInfo);
                hostInfo.port = Integer.toString(portNum);
            }

            for (int i = 0; i < GlobalContext.numLocalCommChannels; i++) {
                int serverId = getServerThreadId(entry.key(), i);
                GlobalContext.serverMap.put(serverId, hostInfo);

                portNum++;
                hostInfo = new HostInfo(hostInfo);
                hostInfo.port = Integer.toString(portNum);

                GlobalContext.serverIds.add(serverId);
            }
        }
    }

    public static int getNumLocalCommChannels() {
        return numLocalCommChannels;
    }

    public static int getNumTotalCommChannels() {
        return numLocalCommChannels * numClients;
    }

    public static int getNumLocalWorkerThreads() {
        return GlobalContext.numLocalWorkerThreads;
    }

    public static int getNumTotalWorkerThreads() {
        return numLocalWorkerThreads * numClients;
    }

    public static int getNumClients() {
        return GlobalContext.numClients;
    }

    public static HostInfo getServerInfo(int serverId) {
        return GlobalContext.serverMap.get(serverId);
    }

    public static HostInfo getNameNodeInfo() {
        return GlobalContext.nameNodeHostInfo;
    }

    public static TIntList getAllServerIds() {
        return GlobalContext.serverIds;
    }

    public static int getClientId() {
        return GlobalContext.clientId;
    }

    public static int getPartitionCommChannelIndex(int rowId) {
        return rowId % GlobalContext.numLocalCommChannels;
    }

    public static int getPartitionClientId(int rowId) {
        return (rowId / numLocalCommChannels) % numClients;
    }

    public static int getPartitionServerId(int rowId, int commChannelIdx) {
        int clientId = getPartitionClientId(rowId);
        return getServerThreadId(clientId, commChannelIdx);
    }

    public static int getCommChannelIndexServer(int serverId) {
        return serverId % MAX_NUM_THREADS_PER_CLIENT
                - SERVER_THREAD_ID_START_OFFSET;
    }

    public static int getLocalIdMin() {
        return localIdMin;
    }

    public static int getLockPoolSize() {
        final int STRIPED_LOCK_EXPANSION_FACTOR = 100;
        return (numLocalWorkerThreads + numLocalCommChannels)
                * STRIPED_LOCK_EXPANSION_FACTOR;
    }

}
