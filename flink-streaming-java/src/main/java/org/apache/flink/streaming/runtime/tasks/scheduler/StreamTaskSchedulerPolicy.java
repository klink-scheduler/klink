package org.apache.flink.streaming.runtime.tasks.scheduler;

public enum StreamTaskSchedulerPolicy {
    OS_SCHEDULER("OS_SCHEDULER"),
    ROUND_ROBIN("ROUND_ROBIN"),
    LONGEST_QUEUE_FIRST("LONGEST_QUEUE_FIRST"),
    SHORTEST_REMAINING_PROCESSING_TIME("SHORTEST_REMAINING_PROCESSING_TIME"),
    RATE_BASED("RATE_BASED"),
    SEWPA("SEWPA"),
    FIRST_COME_FIRST_SERVED("FCFS");

    /* Unique identifier for each policy */
    private final String schedulerName;

    StreamTaskSchedulerPolicy(String schedulerName) {
        this.schedulerName = schedulerName;
    }

    /* String Index mapping to policy */
    public static StreamTaskSchedulerPolicy fromName(String schedulerName) {
        for (StreamTaskSchedulerPolicy policy : values()) {
            if (policy.schedulerName.equalsIgnoreCase(schedulerName)) {
                return policy;
            }
        }
        return null;
    }


    /* Integer index mapping to policy */
    public static StreamTaskSchedulerPolicy fromIndex(int schedulerIndex) throws Exception {
        if (schedulerIndex >= values().length) {
            throw new ArrayIndexOutOfBoundsException("Found index " + schedulerIndex + " but maximum was " + values().length);
        }
        return values()[schedulerIndex];
    }

    public String getSchedulerName() {
        return schedulerName;
    }
}
