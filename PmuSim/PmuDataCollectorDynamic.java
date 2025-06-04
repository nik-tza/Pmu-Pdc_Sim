package PmuSim;

import com.mechalikh.pureedgesim.datacentersmanager.ComputingNode;
import com.mechalikh.pureedgesim.simulationengine.Event;
import com.mechalikh.pureedgesim.simulationengine.SimEntity;
import com.mechalikh.pureedgesim.simulationmanager.SimulationManager;
import com.mechalikh.pureedgesim.taskgenerator.Task;
import com.mechalikh.pureedgesim.locationmanager.Location;
import com.mechalikh.pureedgesim.scenariomanager.SimulationParameters;
import com.mechalikh.pureedgesim.taskgenerator.DefaultTask;

import java.util.*;

/**
 * PMU Data Collector - Generation Time Based Collection
 * Groups tasks by their generation time (getTime()) and waits for max latency period
 * Enhanced with distance-based delays
 */
public class PmuDataCollectorDynamic extends SimEntity {
    
    // Event tags
    public static final int PMU_DATA_RECEIVED = 300;
    public static final int GENERATION_TIME_TIMEOUT = 301;
    
    // Collection parameters
    private final int REQUIRED_PMU_COUNT;
    private static final double MAX_WAITING_LATENCY =  0.0449; // **max waiting for late PMU data**
    
    // Fixed PMU data size (2KB)
    private static final double PMU_DATA_SIZE_KB = 2.0;
    private static final double PMU_DATA_SIZE_BITS = PMU_DATA_SIZE_KB * 8192.0;
    
    // **State Estimation Task Parameters**
    private static final long GRID_ANALYSIS_LENGTH_MI = 15000; // 15,000 MI for complex grid analysis
    private static final double GRID_ANALYSIS_MAX_LATENCY = 2.0; // 2 seconds max latency
    private static final long GRID_ANALYSIS_OUTPUT_SIZE_KB = 50; // 50KB analysis result
    private static final long GRID_ANALYSIS_CONTAINER_SIZE_MB = 100; // 100MB container
    
    // Jitter for realistic network variation  
    private static final Random random = new Random();
    private static final double CELLULAR_JITTER_MS = 5.0;
    private static final double MAN_JITTER_MS = 2.0; // 2ms jitter for MAN
    private static final double WAN_JITTER_MS = 8.0; // 8ms jitter for WAN
    
    // **NEW: Collection buffer for same generation time tasks**
    private final Map<Double, List<TaskWithArrivalTime>> generationTimeBuffer = Collections.synchronizedMap(new HashMap<>());
    private boolean processingGeneration = false;
    
    // TSO reference
    private ComputingNode tsoNode;
    private SimulationManager simulationManager;
    
    // Statistics
    private int totalBatches = 0;
    private int completeBatches = 0;
    private int timeoutBatches = 0;
    private int droppedLateArrivals = 0;
    
    /**
     * PMU Measurement data structure
     */
    public static class PmuMeasurement {
        public final int pmuId;
        public final double arrivalTime;
        public final double generationTime; // **NEW: Task generation time**
        public final double dataSize;
        public final Task originalTask;
        
        public PmuMeasurement(int pmuId, double arrivalTime, double generationTime, double dataSize, Task originalTask) {
            this.pmuId = pmuId;
            this.arrivalTime = arrivalTime;
            this.generationTime = generationTime;
            this.dataSize = dataSize;
            this.originalTask = originalTask;
        }
    }
    
    /**
     * **NEW: Batch for arrival time-based collection window**
     */
    public static class ArrivalTimeBatch {
        public final int batchId;
        public final double firstArrivalTime; // When first PMU arrived (collection window start)
        public final double deadline; // When this batch will timeout
        public final Map<Integer, PmuMeasurement> measurements = Collections.synchronizedMap(new HashMap<>());
        
        public ArrivalTimeBatch(int batchId, double firstArrivalTime, double maxWaitingLatency) {
            this.batchId = batchId;
            this.firstArrivalTime = firstArrivalTime;
            this.deadline = firstArrivalTime + maxWaitingLatency;
        }
    }
    
    public PmuDataCollectorDynamic(SimulationManager simulationManager, ComputingNode tsoNode) {
        super(simulationManager.getSimulation());
        this.simulationManager = simulationManager;
        this.tsoNode = tsoNode;
        this.REQUIRED_PMU_COUNT = SimulationParameters.maxNumberOfEdgeDevices;
        
        System.out.printf("PmuDataCollectorDynamic - Initialized for %d PMUs (distance delays handled by PmuNetworkModel)%n", 
                         REQUIRED_PMU_COUNT);
    }
    
    @Override
    public void processEvent(Event ev) {
        synchronized(this) {
            switch (ev.getTag()) {
                case PMU_DATA_RECEIVED:
                    collectPmuData((Task) ev.getData());
                    break;
                case GENERATION_TIME_TIMEOUT:
                    handleGenerationTimeTimeout((Integer) ev.getData());
                    break;
                default:
                    System.err.println("PmuDataCollectorDynamic - Unknown event: " + ev.getTag());
            }
        }
    }
    
    /**
     * **NEW: Arrival time-based collection logic with proper deadline enforcement**
     */
    public void collectPmuData(Task dataTask) {
        double currentTime = simulationManager.getSimulation().clock();
        double generationTime = dataTask.getTime(); // Task generation time
        
        // Extract PMU information
        int pmuId = extractPmuId(dataTask.getEdgeDevice(), dataTask);
        
        // **Use fixed 2KB data size**
        double dataSize = PMU_DATA_SIZE_BITS;
        double dataSizeKB = PMU_DATA_SIZE_KB;
        
        // Override the task's file size to ensure consistency
        dataTask.setFileSizeInBits((long)dataSize);
        
        // **Get network transfer details from PmuNetworkModel (consistent with actual transfer)**
        PmuNetworkModel networkModel = getNetworkModel();
        PmuNetworkModel.NetworkTransferResult transferResult = null;
        
        if (networkModel != null) {
            ComputingNode tsoNode = findTsoNode();
            transferResult = networkModel.calculateNetworkTransferWithDetails(dataTask.getEdgeDevice(), tsoNode, dataTask, dataSize);
        }
        
        // **Calculate REAL arrival time with network delay**
        double networkDelay = transferResult != null ? transferResult.totalDelay : calculateBasicNetworkDelay(dataSize);
        double realArrivalTime = generationTime + networkDelay;
        
        // **Get path string for logging**
        String path = transferResult != null ? transferResult.getPathString() : 
                     String.format("PMU_%d -> GNB_? -> TELCO -> TSO (basic calculation)", pmuId);
        
        // **Buffer tasks for batch processing**
        if (generationTimeBuffer.get(generationTime) == null) {
            generationTimeBuffer.put(generationTime, new ArrayList<>());
            
            // **FIXED: Use higher precision for fractional generation times**
            // Convert generation time to integer with 1000x precision (0.333 -> 333, 0.667 -> 667)
            int batchId = (int)Math.round(generationTime * 1000);
            
            // **Schedule processing after a short delay to collect all tasks of this generation time**
            schedule(this, 0.001, GENERATION_TIME_TIMEOUT, batchId);
        }
        
        // **Add task to buffer along with its real arrival time**
        TaskWithArrivalTime taskWithTime = new TaskWithArrivalTime(dataTask, realArrivalTime, pmuId, networkDelay, path);
        generationTimeBuffer.get(generationTime).add(taskWithTime);
    }
    
    /**
     * Helper class to store task with its arrival time info
     */
    private static class TaskWithArrivalTime {
        public final Task task;
        public final double realArrivalTime;
        public final int pmuId;
        public final double networkDelay;
        public final String path;
        
        public TaskWithArrivalTime(Task task, double realArrivalTime, int pmuId, double networkDelay, String path) {
            this.task = task;
            this.realArrivalTime = realArrivalTime;
            this.pmuId = pmuId;
            this.networkDelay = networkDelay;
            this.path = path;
        }
    }
    
    /**
     * Processes buffered tasks and applies MAX_WAITING_LATENCY rule
     */
    private void handleGenerationTimeTimeout(int batchId) {
        // Find the generation time for this batch ID
        double targetGenerationTime = batchId / 1000.0;
        
        List<TaskWithArrivalTime> tasks = generationTimeBuffer.get(targetGenerationTime);
        if (tasks == null || tasks.isEmpty()) {
            return;
        }
        
        System.out.printf("PmuDataCollectorDynamic - Processing Generation Time %.3f with %d tasks%n", 
                         targetGenerationTime, tasks.size());
        
        // **Sort tasks by real arrival time**
        tasks.sort((t1, t2) -> Double.compare(t1.realArrivalTime, t2.realArrivalTime));
        
        // **Find first arrival time (chronologically earliest)**
        double firstArrivalTime = tasks.get(0).realArrivalTime;
        double deadline = firstArrivalTime + MAX_WAITING_LATENCY;
        
        // **Apply MAX_WAITING_LATENCY filter**
        List<TaskWithArrivalTime> onTimeTasks = new ArrayList<>();
        int droppedTasks = 0;
        
        for (TaskWithArrivalTime taskWithTime : tasks) {
            if (taskWithTime.realArrivalTime <= deadline) {
                onTimeTasks.add(taskWithTime);
                
                // **Log on-time data transfer**
                PmuLogger.getInstance().logPmuDataTransferFull(
                    taskWithTime.task, taskWithTime.pmuId, PMU_DATA_SIZE_KB, 
                    taskWithTime.path, false, taskWithTime.networkDelay);
                
                System.out.printf("PmuDataCollectorDynamic - ON-TIME: PMU %d at %.4f (deadline: %.4f, margin: %.4fs)%n", 
                                 taskWithTime.pmuId, taskWithTime.realArrivalTime, deadline, 
                                 deadline - taskWithTime.realArrivalTime);
            } else {
                droppedTasks++;
                droppedLateArrivals++;
                
                // **Log late data transfer**
                PmuLogger.getInstance().logPmuDataTransferFull(
                    taskWithTime.task, taskWithTime.pmuId, PMU_DATA_SIZE_KB, 
                    taskWithTime.path, true, taskWithTime.networkDelay);
                
                System.out.printf("PmuDataCollectorDynamic - LATE ARRIVAL: PMU %d at %.4f (deadline: %.4f, late by: %.4fs) - REJECTED%n", 
                                 taskWithTime.pmuId, taskWithTime.realArrivalTime, deadline, 
                                 taskWithTime.realArrivalTime - deadline);
            }
        }
        
        System.out.printf("PmuDataCollectorDynamic - Collection Window Summary: %d on-time, %d dropped (%.1f%% success)%n", 
                         onTimeTasks.size(), droppedTasks, 
                         (onTimeTasks.size() * 100.0) / (onTimeTasks.size() + droppedTasks));
        
        // **Create State Estimation Task with on-time tasks only**
        boolean isComplete = (onTimeTasks.size() >= REQUIRED_PMU_COUNT);
        createStateEstimationTaskFromBuffer(targetGenerationTime, onTimeTasks, isComplete, firstArrivalTime);
        
        // **Clean up buffer**
        generationTimeBuffer.remove(targetGenerationTime);
    }
    
    /**
     * Creates a state estimation task from filtered on-time PMU data
     */
    private void createStateEstimationTaskFromBuffer(double generationTime, List<TaskWithArrivalTime> onTimeTasks, 
                                                   boolean isComplete, double firstArrivalTime) {
        if (onTimeTasks.isEmpty()) {
            System.out.printf("PmuDataCollectorDynamic - No on-time measurements for Generation Time %.3f%n", generationTime);
            return;
        }
        
        totalBatches++;
        
        // Use first task as base for getting application info
        Task baseTask = onTimeTasks.get(0).task;
        
        // Calculate total data size
        double totalDataSize = onTimeTasks.size() * PMU_DATA_SIZE_BITS;
        double totalDataKB = totalDataSize / 8192.0;
        
        // **CALCULATE PDC WAITING TIME** 
        List<Double> arrivalTimes = onTimeTasks.stream()
                                              .map(t -> t.realArrivalTime)
                                              .sorted()
                                              .collect(java.util.stream.Collectors.toList());
        
        double pdcWaitingTime = 0.0;
        
        if (onTimeTasks.size() >= REQUIRED_PMU_COUNT) {
            // **ALL PMU ARRIVED ON TIME - PDC finished early**
            // PDC Waiting = time from first to last arrival
            if (arrivalTimes.size() > 1) {
                pdcWaitingTime = arrivalTimes.get(arrivalTimes.size() - 1) - arrivalTimes.get(0);
                
                System.out.printf("PmuDataCollectorDynamic - ALL PMU ON TIME: PDC Waiting Time: %.4fs (from %.4fs to %.4fs), Finished early!%n", 
                                 pdcWaitingTime, arrivalTimes.get(0), arrivalTimes.get(arrivalTimes.size() - 1));
            }
        } else {
            // **NOT ALL PMU ARRIVED ON TIME - PDC waited full MAX_WAITING_LATENCY**
            // PDC Waiting = MAX_WAITING_LATENCY (PDC waited the full timeout period)
            pdcWaitingTime = MAX_WAITING_LATENCY;
            
            System.out.printf("PmuDataCollectorDynamic - INCOMPLETE ARRIVAL: PDC Waiting Time: %.4fs (waited full timeout), Got %d/%d PMUs%n", 
                             pdcWaitingTime, onTimeTasks.size(), REQUIRED_PMU_COUNT);
        }
        
        // **VERIFY PDC waiting time logic**
        if (onTimeTasks.size() >= REQUIRED_PMU_COUNT && pdcWaitingTime > MAX_WAITING_LATENCY) {
            System.err.printf("ERROR: PDC Waiting Time %.4fs exceeds MAX_WAITING_LATENCY %.4fs even with all PMUs!%n", 
                             pdcWaitingTime, MAX_WAITING_LATENCY);
        }
        
        // Create state estimation task description
        String batchType = isComplete ? "COMPLETE" : "TIMEOUT";
        String taskDescription = String.format("GRID_ANALYSIS:Window_%.3f:%d/%d:%s", 
                                             generationTime, onTimeTasks.size(), REQUIRED_PMU_COUNT, batchType);
        
        // Create new analysis task
        Task analysisTask;
        try {
            int newTaskId = (int)(baseTask.getId() + 10000 + (generationTime * 1000));
            analysisTask = new com.mechalikh.pureedgesim.taskgenerator.DefaultTask(newTaskId);
            
            // **SET Grid Analysis specific properties**
            analysisTask.setType(taskDescription);
            analysisTask.setOffloadingDestination(tsoNode);
            analysisTask.setEdgeDevice(tsoNode); // TSO as source
            analysisTask.setRegistry(baseTask.getRegistry());
            analysisTask.setOrchestrator(tsoNode); // TSO orchestrates itself
            analysisTask.setApplicationID(baseTask.getApplicationID());
            
            // **Grid Analysis specific parameters**
            analysisTask.setMaxLatency(GRID_ANALYSIS_MAX_LATENCY); // 2 seconds for complex grid analysis
            analysisTask.setTime(simulationManager.getSimulation().clock());
            
            // **Set computational requirements for Grid Analysis**
            analysisTask.setLength(GRID_ANALYSIS_LENGTH_MI); // 15,000 MI for intensive grid computation
            
            // Set container size for Grid Analysis algorithm
            analysisTask.setContainerSizeInBits(GRID_ANALYSIS_CONTAINER_SIZE_MB * 8 * 1024 * 1024); // 100MB in bits
            
            // Set file sizes for analysis task
            analysisTask.setFileSizeInBits((long)totalDataSize); // Input is filtered PMU data
            analysisTask.setOutputSizeInBits(GRID_ANALYSIS_OUTPUT_SIZE_KB * 8192); // 50KB analysis result
            
            // **LOG grid analysis task creation**
            PmuLogger.getInstance().logStateEstimationTaskCreation(
                analysisTask, (int)(generationTime * 1000), onTimeTasks.size(), REQUIRED_PMU_COUNT, 
                pdcWaitingTime, totalDataKB, batchType
            );
            
            // Log data collection completion
            PmuLogger.getInstance().logDataCollectionComplete(generationTime, onTimeTasks.size(), isComplete);
            
            // **EXECUTE the grid analysis task immediately**
            scheduleNow(simulationManager, com.mechalikh.pureedgesim.simulationmanager.SimulationManager.SEND_TO_ORCH, analysisTask);
            
            System.out.printf("PmuDataCollectorDynamic - Created Grid Analysis Task %d for Generation Time %.3f (%d/%d PMUs, %.4fs PDC wait, %.2f KB data)%n",
                             newTaskId, generationTime, onTimeTasks.size(), REQUIRED_PMU_COUNT, pdcWaitingTime, totalDataKB);
            
            if (isComplete) {
                completeBatches++;
            } else {
                timeoutBatches++;
            }
            
        } catch (Exception e) {
            System.err.println("PmuDataCollectorDynamic - Error creating grid analysis task: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Extract PMU ID from edge device or task
     */
    private int extractPmuId(ComputingNode edgeDevice, Task dataTask) {
        if (edgeDevice != null) {
            // Try to extract from device name
            try {
                String deviceName = edgeDevice.getName();
                if (deviceName != null && deviceName.contains("_")) {
                    return Integer.parseInt(deviceName.split("_")[1]);
                }
            } catch (Exception e) {
                // Fall through to task-based extraction
            }
            
            // Fallback: use device position in mist devices list
            try {
                List<ComputingNode> pmuDevices = simulationManager.getDataCentersManager()
                        .getComputingNodesGenerator().getMistOnlyList();
                for (int i = 0; i < pmuDevices.size(); i++) {
                    if (pmuDevices.get(i).equals(edgeDevice)) {
                        return i;
                    }
                }
            } catch (Exception e) {
                System.err.println("PmuDataCollectorDynamic - Error finding PMU ID from device list: " + e.getMessage());
            }
        }
        
        // Final fallback: extract from task ID
        return extractPmuIdFromTask(dataTask);
    }
    
    /**
     * Extract PMU ID from task properties
     */
    private int extractPmuIdFromTask(Task dataTask) {
        try {
            // Use task ID modulo to distribute across available PMU range
            return (int)(dataTask.getId() % REQUIRED_PMU_COUNT);
        } catch (Exception e) {
            System.err.println("PmuDataCollectorDynamic - Error extracting PMU ID from task: " + e.getMessage());
            return 0; // Default fallback
        }
    }
    
    /**
     * Get the PmuNetworkModel instance from simulation manager
     */
    private PmuNetworkModel getNetworkModel() {
        try {
            // Get the network model from simulation manager
            if (simulationManager.getNetworkModel() instanceof PmuNetworkModel) {
                return (PmuNetworkModel) simulationManager.getNetworkModel();
            }
        } catch (Exception e) {
            System.err.println("PmuDataCollectorDynamic - Could not get PmuNetworkModel: " + e.getMessage());
        }
        return null;
    }
    
    /**
     * Basic network delay calculation as fallback
     */
    private double calculateBasicNetworkDelay(double dataSize) {
        // Basic calculation: transfer time + latency + minimal jitter
        double pmuToGnbTime = dataSize / SimulationParameters.cellularBandwidthBitsPerSecond + 
                             SimulationParameters.cellularLatency + (random.nextDouble() * 0.005);
        
        double gnbToTelcoTime = dataSize / SimulationParameters.manBandwidthBitsPerSecond + 
                               SimulationParameters.manLatency + (random.nextDouble() * 0.002);
        
        double telcoToTsoTime = dataSize / SimulationParameters.wanBandwidthBitsPerSecond + 
                               SimulationParameters.wanLatency + (random.nextDouble() * 0.008);
        
        return pmuToGnbTime + gnbToTelcoTime + telcoToTsoTime;
    }
    
    /**
     * Find TSO node (minimal implementation for compilation)
     */
    private ComputingNode findTsoNode() {
        try {
            List<ComputingNode> cloudDatacenters = simulationManager.getDataCentersManager()
                                                                   .getComputingNodesGenerator()
                                                                   .getCloudOnlyList();
            
            for (ComputingNode cloud : cloudDatacenters) {
                if (cloud.getName() != null && cloud.getName().equals("TSO")) {
                    return cloud;
                }
            }
            
            // Fallback to first cloud datacenter
            if (!cloudDatacenters.isEmpty()) {
                return cloudDatacenters.get(0);
            }
        } catch (Exception e) {
            // Silent fallback
        }
        return null;
    }
    
    /**
     * Get statistics for logging
     */
    public String getStatistics() {
        return String.format("Generation Time Collection Windows: %d total, %d complete, %d timeout, %d late arrivals dropped",
                           totalBatches, completeBatches, timeoutBatches, droppedLateArrivals);
    }
} 