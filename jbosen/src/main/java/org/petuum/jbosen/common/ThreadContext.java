package org.petuum.jbosen.common;

public class ThreadContext {

    private static ThreadLocal<Info> thrInfo = new ThreadLocal<>();

    public static void registerThread(int threadId) {
        thrInfo.set(new Info(threadId));
    }

    public static int getId() {
        return thrInfo.get().entityId;
    }

    public static int getClock() {
        return thrInfo.get().clock;
    }

    public static void clock() {
        thrInfo.get().clock++;
    }

    public static int getCachedSystemClock() {
        return thrInfo.get().cachedSystemClock;
    }

    public static void setCachedSystemClock(int systemClock) {
        thrInfo.get().cachedSystemClock = systemClock;
    }

    private static class Info {

        public final int entityId;
        public int clock;
        public int cachedSystemClock;

        public Info(int entityId) {
            this.entityId = entityId;
            this.clock = 0;
            this.cachedSystemClock = 0;
        }
    }
}
