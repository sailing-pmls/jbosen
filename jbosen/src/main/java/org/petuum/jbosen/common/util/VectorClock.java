package org.petuum.jbosen.common.util;

import com.google.common.util.concurrent.AtomicLongMap;

import java.util.Map;
import java.util.Vector;

public class VectorClock {
    private AtomicLongMap<Integer> vecClock;
    private int minClock;

    public VectorClock() {
        minClock = -1;
        vecClock = AtomicLongMap.create();
    }

    public VectorClock(Vector<Integer> ids) {
        minClock = 0;
        vecClock = AtomicLongMap.create();
        for (Integer id : ids) {
            vecClock.put(id, 0);
        }
    }

    private boolean isUniqueMin(int id) {
        if (vecClock.get(id) != minClock) {
            // definitely not the slowest
            return false;
        }
        // check if it is also unique
        int numMin = 0;
        for (Map.Entry<Integer, Long> entry : vecClock.asMap().entrySet()) {
            if (entry.getValue() == minClock)
                ++numMin;
            if (numMin > 1)
                return false;
        }
        return true;
    }

    public void addClock(int id, int clock) {
        vecClock.put(id, clock);
        if (minClock == -1 || clock < minClock)
            minClock = clock;
    }

    public int tick(int id) {
        if (isUniqueMin(id)) {
            vecClock.incrementAndGet(id);
            return ++minClock;
        }
        vecClock.incrementAndGet(id);
        return 0;
    }

    public int tickUntil(int id, int clock) {
        int currClock = getClock(id);
        int numTicks = clock - currClock;
        int newClock = 0;

        for (int i = 0; i < numTicks; ++i) {
            int clockChanged = tick(id);
            if (clockChanged != 0)
                newClock = clockChanged;
        }

        return newClock;
    }

    public int getClock(int id) {
        return (int) vecClock.get(id);
    }

    public int getMinClock() {
        return minClock;
    }
}