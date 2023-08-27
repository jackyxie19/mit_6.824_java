package tools;

public class SnowflakeGenerator {
    private static final long START_TIMESTAMP = 1622505600000L; // 设置起始时间戳，如：2021-06-01 00:00:00

    private static final long MACHINE_ID_BITS = 10L;
    private static final long SEQUENCE_BITS = 12L;

    private static final long MAX_MACHINE_ID = ~(-1L << MACHINE_ID_BITS);
    private static final long MAX_SEQUENCE = ~(-1L << SEQUENCE_BITS);

    private static final long MACHINE_ID_SHIFT = SEQUENCE_BITS;
    private static final long TIMESTAMP_SHIFT = MACHINE_ID_BITS + SEQUENCE_BITS;

    private long machineId;
    private long sequence = 0L;
    private long lastTimestamp = -1L;

    public SnowflakeGenerator(long machineId) {
        if (machineId < 0 || machineId > MAX_MACHINE_ID) {
            throw new IllegalArgumentException("Invalid machineId");
        }
        this.machineId = machineId;
    }

    public synchronized String generateIdWithSpin() {
        int maxTryTime = 100;
        int start = 0;
        while (start++ < maxTryTime) {
            try {
                return generateId();
            } catch (Exception e) {
                try {
                    Thread.sleep(1L);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
        // 失败后最后尝试一次
        return generateId();
    }

    public synchronized String generateId() {
        long currentTimestamp = System.currentTimeMillis();

        if (currentTimestamp < lastTimestamp) {
            throw new RuntimeException("Clock moved backwards. Refusing to generate id.");
        }

        if (currentTimestamp == lastTimestamp) {
            sequence = (sequence + 1) & MAX_SEQUENCE;
            if (sequence == 0) {
                currentTimestamp = waitNextMillis(currentTimestamp);
            }
        } else {
            sequence = 0L;
        }

        lastTimestamp = currentTimestamp;
        long l = ((currentTimestamp - START_TIMESTAMP) << TIMESTAMP_SHIFT)
                | (machineId << MACHINE_ID_SHIFT)
                | sequence;
        return String.valueOf(l);
    }

    private long waitNextMillis(long currentTimestamp) {
        while (currentTimestamp <= lastTimestamp) {
            currentTimestamp = System.currentTimeMillis();
        }
        return currentTimestamp;
    }

    public static void main(String[] args) {
        System.out.println(MAX_MACHINE_ID);
        System.out.println(0x3FF);
        System.out.println(~(-1 << 11));
    }
}