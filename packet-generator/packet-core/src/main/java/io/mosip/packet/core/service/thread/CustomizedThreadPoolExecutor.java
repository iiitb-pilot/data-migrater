package io.mosip.packet.core.service.thread;

import io.mosip.kernel.core.logger.spi.Logger;
import io.mosip.packet.core.constant.activity.ActivityName;
import io.mosip.packet.core.logger.DataProcessLogger;
import io.mosip.packet.core.util.FixedListQueue;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.mosip.packet.core.constant.GlobalConfig.*;

public class CustomizedThreadPoolExecutor {
    List<ThreadPoolExecutor> poolMap = new ArrayList<>();
    private int MAX_THREAD_EXE_COUNT;
    private Long DELAY_SECONDS = 60000L;
    private int maxThreadCount;
    private boolean noSlotAvailable=false;
    private AtomicLong totalTaskCount = new AtomicLong();
    private AtomicLong totalCompletedTaskCount = new AtomicLong();
    private AtomicLong failedRecordCount = new AtomicLong();
    private AtomicLong completedCount = new AtomicLong();
    private AtomicLong currentPendingCount = new AtomicLong();
    private AtomicLong countOfZeroActiveCount = new AtomicLong();
    private LocalDateTime threadStart = LocalDateTime.now();
    private static final Logger LOGGER = DataProcessLogger.getLogger(CustomizedThreadPoolExecutor.class);

    public int getCountOfZeroActiveCount() {
        return countOfZeroActiveCount.intValue();
    }

    public long getFailedRecordCount() {
        return failedRecordCount.get();
    }

    public void increaseFailedRecordCount() {
        failedRecordCount.incrementAndGet();
        TOTAL_FAILED_RECORDS++;
    }

    private Timer watch = null;
    private Timer estimateTimer = null;
    private Timer slotAllocationTimer = null;
    private String NAME;
    private FixedListQueue<Long> timeConsumptionPerMin = new FixedListQueue<>(100);
    private FixedListQueue<Integer> countOfProcessPerMin = new FixedListQueue<>(100);
    private boolean isInputProcessCompleted = false;
    private boolean isCompletionCountRequired = false;
    private String trackActivityForCompletion = null;

    public long getTotalCompletedTaskCount() {
        return totalCompletedTaskCount.get();
    }

    public long getTotalTaskCount() {
        return totalTaskCount.get();
    }

    public String getNAME() {
        return NAME;
    }

    CountIncrementer failedIncrement = new CountIncrementer() {
        @Override
        public void increment() {
            increaseFailedRecordCount();
        }
    };

    public CustomizedThreadPoolExecutor(Integer threadPoolCount, Integer maxThreadCount, Integer maxThreadExecCount, String poolName) {
        this(threadPoolCount, maxThreadCount, maxThreadExecCount, poolName, true, null);
    }

    public CustomizedThreadPoolExecutor(Integer threadPoolCount, Integer maxThreadCount, Integer maxThreadExecCount, String poolName, Boolean monitorRequired) {
        this(threadPoolCount, maxThreadCount, maxThreadExecCount, poolName, monitorRequired, null);
    }

    public CustomizedThreadPoolExecutor(Integer threadPoolCount, Integer maxThreadCount, Integer maxThreadExecCount, String poolName, Boolean monitorRequired, ActivityName trackActivity) {
        this.NAME = poolName;
        this.maxThreadCount = maxThreadCount;
        this.MAX_THREAD_EXE_COUNT = maxThreadExecCount;

        if(trackActivity != null) {
            this.isCompletionCountRequired = true;
            this.trackActivityForCompletion = trackActivity.getActivityName();
            COMPLETION_COUNT_MAP.put(this.trackActivityForCompletion, Long.valueOf(0L));
        }

        for(int i = 1; i <= threadPoolCount; i++)
            poolMap.add((ThreadPoolExecutor) Executors.newFixedThreadPool(MAX_THREAD_EXE_COUNT));

        slotAllocationTimer = new Timer("Slot Allocation Timer");
        slotAllocationTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    if(noSlotAvailable) {
                        boolean isSuccess = false;
                        List<ThreadPoolExecutor> poolMap1 = new ArrayList<>();
                        List<Integer> removeIndex = new ArrayList<>();
                        for(int i=0; i < poolMap.size(); i++) {
                            ThreadPoolExecutor entry = poolMap.get(i);
                            if(entry.getActiveCount() ==0 && entry.getTaskCount() > 0 && entry.getCompletedTaskCount() > 0 && entry.getTaskCount() == entry.getCompletedTaskCount()) {
                                totalTaskCount.addAndGet(entry.getTaskCount());
                                totalCompletedTaskCount.addAndGet(entry.getCompletedTaskCount());
                                removeIndex.add(i);
                                poolMap1.add((ThreadPoolExecutor) Executors.newFixedThreadPool(MAX_THREAD_EXE_COUNT));
                                isSuccess=true;
                            }
                        }

                        Collections.sort(removeIndex, new Comparator<Integer>() {
                            @Override
                            public int compare(Integer o1, Integer o2) {
                                return o2.compareTo(o1);
                            }
                        });

                        for(int i : removeIndex) {
                            ThreadPoolExecutor entry = poolMap.get(i);
                            entry.shutdown();
                            entry.purge();
                            poolMap.remove(i);
                        }

                        if(poolMap1.size() > 0)
                            poolMap.addAll(poolMap1);


                        if(isSuccess)
                            noSlotAvailable=false;
                    }

                    try {
                        Collections.sort(poolMap, new SortbyCount());
                    } catch (ConcurrentModificationException e){}
                } catch (Exception e) {}
            }
        }, 0, 7000L);

        if(monitorRequired) {
            estimateTimer = new Timer("Estimate Time Calculator");
            estimateTimer.schedule(new TimerTask() {
                @Override
                public void run() {
                    try {
                        if (TIMECONSUPTIONQUEUE != null && TIMECONSUPTIONQUEUE.size() > 0) {
                            FixedListQueue<Long> listQueue = (FixedListQueue<Long>) TIMECONSUPTIONQUEUE.clone();
                            TIMECONSUPTIONQUEUE.clear();

                            Long avgTime = 0l;
                            Long[] consumedTimeList = listQueue.toArray(new Long[listQueue.size()]);

                            Long TotalSum = Arrays.stream(consumedTimeList).mapToLong(Long::longValue).sum();
                            int noOfRecords = consumedTimeList.length;
                            if(noOfRecords > 0)
                                avgTime = TotalSum / noOfRecords;

                            timeConsumptionPerMin.add(avgTime);
                            countOfProcessPerMin.add(noOfRecords);

                        }
                    } catch (Exception e){}

                }
            }, 0, DELAY_SECONDS);
        }

        watch = new Timer("ThreadPool_Wathcer");
        watch.schedule(new TimerTask() {
            @Override
            public void run() {
                Long totalCount = 0L;
                Long activeCount = 0L;
                completedCount.set(0L);
                Long avgTime = 0l;
                int avgCount = 0;


                try {
                    for(ThreadPoolExecutor entry : poolMap) {
                        totalCount += entry.getTaskCount();
                        activeCount+= entry.getActiveCount();
                        completedCount.addAndGet(entry.getCompletedTaskCount());
                    }

                    if(activeCount <= 0)
                        countOfZeroActiveCount.incrementAndGet();
                    else
                        countOfZeroActiveCount.set(0L);

                    completedCount.addAndGet(totalCompletedTaskCount.get());

                    if((totalTaskCount.get() > 0 || totalCount > 0) && monitorRequired) {
                        int totalYears = 0;
                        int totalMonths = 0;
                        int totalDays = 0;
                        long totalHours = 0;
                        long remainingMinutes =0;

                        int totalElapsedYears = 0;
                        int totalElapsedMonths = 0;
                        int totalElapsedDays = 0;
                        long totalElapsedHours = 0L;
                        long remainingElapsedMinutes = 0L;

                        // Calculating Estimated Time of Process Completion
                        if(timeConsumptionPerMin != null && timeConsumptionPerMin.size() > 0) {
                            FixedListQueue<Long> listQueue = (FixedListQueue<Long>) timeConsumptionPerMin.clone();
                            FixedListQueue<Integer> countQueue = (FixedListQueue<Integer>)countOfProcessPerMin.clone();

                            Long[] consumedTimeList = listQueue.toArray(new Long[listQueue.size()]);
                            Long totalRecords = TOTAL_RECORDS_FOR_PROCESS;
                            long TotalSum = Arrays.stream(consumedTimeList).mapToLong(Long::longValue).sum();
                            int noOfRecords = consumedTimeList.length;

                            Integer[] consumedCountList = countQueue.toArray(new Integer[countQueue.size()]);
                            int TotalCountSum = Arrays.stream(consumedCountList).mapToInt(Integer::intValue).sum();
                            int noOfCountRecords = consumedCountList.length;
                            avgCount = TotalCountSum/noOfCountRecords;

                            long remainingRecords = totalRecords - (completedCount.addAndGet(failedRecordCount.get()));
                            avgTime = TotalSum / noOfRecords;
                            long totalTimeRequired = (remainingRecords / avgCount);

                            LocalDateTime currentTime = LocalDateTime.now();
                            // Calculate Remaining Time required
                            LocalDateTime end = currentTime.plusMinutes(totalTimeRequired);
                            Period dateDiff = Period.between(currentTime.toLocalDate(), end.toLocalDate());
                            LocalDateTime intermediate = currentTime.plus(dateDiff);

                            if(intermediate.isAfter(end)) {
                                dateDiff = dateDiff.minusDays(1);
                                intermediate = currentTime.plus(dateDiff);
                            }

                            Duration timeDiff = Duration.between(intermediate, end);
                            totalYears = dateDiff.getYears();
                            totalMonths = dateDiff.getMonths();
                            totalDays = dateDiff.getDays();
                            totalHours = timeDiff.toHours();
                            remainingMinutes = timeDiff.minusHours(totalHours).toMinutes();

                            // Calculate Elapsed Time
                            Period elapsedDateDiff = Period.between(threadStart.toLocalDate(), currentTime.toLocalDate());
                            LocalDateTime elapsedDateDiffIntermediate = threadStart.plus(elapsedDateDiff);

                            if(elapsedDateDiffIntermediate.isAfter(currentTime)) {
                                elapsedDateDiff = elapsedDateDiff.minusDays(1);
                                elapsedDateDiffIntermediate = threadStart.plus(elapsedDateDiff);
                            }

                            Duration elapsedTimeDiff = Duration.between(elapsedDateDiffIntermediate, currentTime);
                            totalElapsedYears = elapsedDateDiff.getYears();
                            totalElapsedMonths = elapsedDateDiff.getMonths();
                            totalElapsedDays = elapsedDateDiff.getDays();
                            totalElapsedHours = elapsedTimeDiff.toHours();
                            remainingElapsedMinutes = elapsedTimeDiff.minusHours(totalElapsedHours).toMinutes();
                        }

                        long totalTaskCnt = totalTaskCount.get() + totalCount;
                        System.out.println("Pool Name : " + NAME + " Avg Count per Min.: " + avgCount + " Avg Time per Record : " + TimeUnit.MILLISECONDS.convert(avgTime, TimeUnit.NANOSECONDS) + "S  Time Elapsed : " + totalElapsedYears + "Y " + totalElapsedMonths + "M " + totalElapsedDays + "D " + totalElapsedHours + "H " + remainingElapsedMinutes + "M" + "  Estimate Time of Completion : " + totalYears + "Y " + totalMonths + "M " + totalDays + "D " + totalHours + "H " + remainingMinutes + "M" +"  Total Records for Process : " + TOTAL_RECORDS_FOR_PROCESS + " Failed in Previous Batch : " + TOTAL_FAILED_RECORDS + "  Total Task : " + totalTaskCnt  + ", Active Task : " + activeCount + ", Completed Task : " + completedCount + ", Failed Task : " + failedRecordCount + (isCompletionCountRequired ? ", No of "+ trackActivityForCompletion + " Completed : " +  COMPLETION_COUNT_MAP.get(trackActivityForCompletion) : ""));
                        LOGGER.info("Pool Name : " + NAME + " Avg Count per Min.: " + avgCount + " Avg Time per Record : " + TimeUnit.MILLISECONDS.convert(avgTime, TimeUnit.NANOSECONDS) + "S  Time Elapsed : " + totalElapsedYears + "Y " + totalElapsedMonths + "M " + totalElapsedDays + "D " + totalElapsedHours + "H " + remainingElapsedMinutes + "M" + "  Estimate Time of Completion : " + totalYears + "Y " + totalMonths + "M " + totalDays + "D " + totalHours + "H " + remainingMinutes + "M" +"  Total Records for Process : " + TOTAL_RECORDS_FOR_PROCESS + " Failed in Previous Batch : " + TOTAL_FAILED_RECORDS + "  Total Task : " + totalTaskCnt  + ", Active Task : " + activeCount + ", Completed Task : " + completedCount + ", Failed Task : " + failedRecordCount + (isCompletionCountRequired ? ", No of "+ trackActivityForCompletion + " Completed : " + COMPLETION_COUNT_MAP.get(trackActivityForCompletion) : ""));
                    }    } catch (Exception e) {}
            }
        }, 0, 120000L);

        THREAD_POOL_EXECUTOR_LIST.add(this);
    }

    public synchronized void ExecuteTask(BaseThreadController task) throws InterruptedException {
        boolean taskAdded = false;
        task.setPoolName(NAME);
        task.setFailedRecordCount(failedIncrement);

        do {
            if(!noSlotAvailable) {
                for(ThreadPoolExecutor entry : poolMap) {
                    if(entry.getTaskCount() <= maxThreadCount) {
                        task.setResponse(new BaseThreadController.SuuccessResponse() {
                            @Override
                            public void onSuccess() {
                                currentPendingCount.decrementAndGet();

                                if(COMPLETION_COUNT_MAP.containsKey(NAME)) {
                                    Long count = COMPLETION_COUNT_MAP.get(NAME);
                                    count++;
                                    COMPLETION_COUNT_MAP.put(NAME, count);
                                }
                            }

                            @Override
                            public void onFailure() {
                                currentPendingCount.decrementAndGet();
                            }
                        });
                        currentPendingCount.incrementAndGet();
                        entry.execute(task);
                        taskAdded=true;
                        break;
                    }
                }

                boolean slotAvailable = false;
                for(ThreadPoolExecutor entry : poolMap) {

                    if(entry.getTaskCount() < maxThreadCount) {
                        slotAvailable = true;
                    }
                }

                if(!slotAvailable) {
                    noSlotAvailable = true;
                }
            }  else {
                TimeUnit.SECONDS.sleep(10);
            }
        } while ((noSlotAvailable && !taskAdded) || !taskAdded);

    }

    class SortbyCount implements Comparator<ThreadPoolExecutor> {
        public int compare(ThreadPoolExecutor a, ThreadPoolExecutor b)
        {
            return Long.valueOf(a.getTaskCount()).compareTo(Long.valueOf(b.getTaskCount()));
        }
    }

    public boolean isBatchAcceptRequest() {
        return !noSlotAvailable;
    }

    public List<ThreadPoolExecutor> getPoolMap() {
        return poolMap;
    }

    public void setInputProcessCompleted(Boolean isCompleted) {
        this.isInputProcessCompleted = isCompleted;
    }

    public boolean getInputProcessCompleted() {
        return this.isInputProcessCompleted;
    }

    public Timer getSlotAllocationTimer() {
        return slotAllocationTimer;
    }

    public void setSlotAllocationTimer(Timer slotAllocationTimer) {
        this.slotAllocationTimer = slotAllocationTimer;
    }

    public Timer getWatch() {
        return watch;
    }

    public void setWatch(Timer watch) {
        this.watch = watch;
    }

    public Timer getEstimateTimer() {
        return estimateTimer;
    }

    public void setEstimateTimer(Timer estimateTimer) {
        this.estimateTimer = estimateTimer;
    }

    public Long getCurrentCompletedTask() {
        return completedCount.get();
    }

    public Long getCurrentPendingCount() {
        return currentPendingCount.get();
    }
}
