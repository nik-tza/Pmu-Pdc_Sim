package UpfOnTelco_PdcOnEdge;

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
 * GNB Data Collector - Per-GNB Generation Time Based Collection
 * Each GNB instance groups tasks from its assigned PMUs by generation time
 * Enhanced with distance-based delays and distributed processing
 */
public class UpfTelcoDataCollectorDynamic extends SimEntity {
    
    // Event tags
    public static final int UPFTELCO_DATA_RECEIVED = 300;
    public static final int GENERATION_TIME_TIMEOUT = 301;
    
    // Collection parameters  
    private final int REQUIRED_UPFTELCO_COUNT; // PMUs assigned to this GNB
    private static final double MAX_WAITING_LATENCY = 0.0155; // **max waiting for late UpfTelco data**
    
    // **NEW: GNB-specific properties**
    private final ComputingNode assignedGnb; // The GNB this collector belongs to
    private final String gnbId; // GNB identifier (e.g., "1", "2", "3")
    
    // Fixed UpfTelco data size (2KB)
    private static final double UPFTELCO_DATA_SIZE_KB = 2.0;
    private static final double UPFTELCO_DATA_SIZE_BITS = UPFTELCO_DATA_SIZE_KB * 8192.0;
    
    // **State Estimation Task Parameters**
    private static final long GRID_ANALYSIS_LENGTH_MI = 1000; // 15,000 MI for complex grid analysis
    private static final double GRID_ANALYSIS_MAX_LATENCY = 2.0; // 2 seconds max latency
    private static final long GRID_ANALYSIS_OUTPUT_SIZE_KB = 50; // 50KB analysis result
    private static final long GRID_ANALYSIS_CONTAINER_SIZE_MB = 100; // 100MB container
    
    // Jitter for realistic network variation  
    private static final Random random = new Random();
    private static final double CELLULAR_JITTER_MS = 5.0;
    private static final double MAN_JITTER_MS = 2.0; // 2ms jitter for MAN
    private static final double WAN_JITTER_MS = 8.0; // 8ms jitter for WAN
    
    // **NEW: Collection buffer for same generation time tasks**
    private final Map<Double, List<TaskWithArrivalTime>> generationTimeBuffer = new HashMap<>();
    
    // **THREAD-SAFETY: Single lock object for all synchronization**
    private final Object dataCollectionLock = new Object();
    
    // TSO reference
    private ComputingNode tsoNode;
    private SimulationManager simulationManager;
    
    // Statistics
    private int totalBatches = 0;
    private int completeBatches = 0;
    private int timeoutBatches = 0;
    private int droppedLateArrivals = 0;
    
    /**
     * UpfTelco Measurement data structure
     */
    public static class UpfTelcoMeasurement {
        public final int upfTelcoId;
        public final double arrivalTime;
        public final double generationTime; // **NEW: Task generation time**
        public final double dataSize;
        public final Task originalTask;
        
        public UpfTelcoMeasurement(int upfTelcoId, double arrivalTime, double generationTime, double dataSize, Task originalTask) {
            this.upfTelcoId = upfTelcoId;
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
        public final double firstArrivalTime; // When first UpfTelco arrived (collection window start)
        public final double deadline; // When this batch will timeout
        public final Map<Integer, UpfTelcoMeasurement> measurements = Collections.synchronizedMap(new HashMap<>());
        
        public ArrivalTimeBatch(int batchId, double firstArrivalTime, double maxWaitingLatency) {
            this.batchId = batchId;
            this.firstArrivalTime = firstArrivalTime;
            this.deadline = firstArrivalTime + maxWaitingLatency;
        }
    }
    
    public UpfTelcoDataCollectorDynamic(SimulationManager simulationManager, ComputingNode assignedGnb) {
        super(simulationManager.getSimulation());
        this.simulationManager = simulationManager;
        this.assignedGnb = assignedGnb;
        this.tsoNode = null; // No longer needed for per-GNB processing
        
        // Extract GNB ID from name (e.g., "EDGE_1" -> "1")
        this.gnbId = extractGnbId(assignedGnb.getName());
        
        // Calculate PMUs assigned to this GNB (approximately equal distribution)
        int totalPmus = SimulationParameters.maxNumberOfEdgeDevices;
        int totalGnbs = getGnbCount();
        this.REQUIRED_UPFTELCO_COUNT = (int) Math.ceil((double) totalPmus / totalGnbs);
        
        System.out.printf("UpfTelcoDataCollectorDynamic - Initialized for GNB_%s expecting ~%d PMUs (of %d total PMUs)%n", 
                         gnbId, REQUIRED_UPFTELCO_COUNT, totalPmus);
    }
    
    /**
     * **NEW: Constructor with explicit PMU count (preferred for accurate expectations)**
     */
    public UpfTelcoDataCollectorDynamic(SimulationManager simulationManager, ComputingNode assignedGnb, int expectedPmuCount) {
        super(simulationManager.getSimulation());
        this.simulationManager = simulationManager;
        this.assignedGnb = assignedGnb;
        this.tsoNode = null; // Not needed for GNB-based processing
        
        // Extract GNB ID from name (e.g., "EDGE_1" -> "1")
        this.gnbId = extractGnbId(assignedGnb.getName());
        
        // Use the exact PMU count assigned to this GNB
        this.REQUIRED_UPFTELCO_COUNT = expectedPmuCount;
        
        System.out.printf("UpfTelcoDataCollectorDynamic - Initialized for GNB_%s expecting %d assigned PMUs%n", 
                         gnbId, REQUIRED_UPFTELCO_COUNT);
    }
    
    @Override
    public void processEvent(Event ev) {
        // Handle events with proper synchronization
        switch (ev.getTag()) {
            case UPFTELCO_DATA_RECEIVED:
                collectUpfTelcoData((Task) ev.getData());
                break;
            case GENERATION_TIME_TIMEOUT:
                handleGenerationTimeTimeout((Integer) ev.getData());
                break;
            default:
                System.err.println("UpfTelcoDataCollectorDynamic - Unknown event: " + ev.getTag());
        }
    }
    
    /**
     * **NEW: GNB-specific data collection with PMU filtering for 3-hop path**
     * Collection window starts when data returns from TELCO to GNB
     */
    public void collectUpfTelcoData(Task dataTask) {
        synchronized(dataCollectionLock) {
            double currentTime = simulationManager.getSimulation().clock();
            double generationTime = dataTask.getTime(); // Task generation time
            
            // Extract UpfTelco information
            int upfTelcoId = extractUpfTelcoId(dataTask.getEdgeDevice(), dataTask);
            
            // **FILTER: Only accept PMU data intended for this GNB**
            if (!isPmuAssignedToThisGnb(dataTask.getEdgeDevice())) {
                System.out.printf("UpfTelcoDataCollectorDynamic - GNB_%s rejected PMU data from UpfTelco_%d (not assigned to this GNB)%n", 
                                 gnbId, upfTelcoId);
                return;
            }
            
            // **Use fixed 2KB data size**
            double dataSize = UPFTELCO_DATA_SIZE_BITS;
            double dataSizeKB = UPFTELCO_DATA_SIZE_KB;
            
            // Override the task's file size to ensure consistency
            dataTask.setFileSizeInBits((long)dataSize);
            
            // **Get network transfer details from UpfTelcoNetworkModel (3-hop path)**
            UpfTelcoNetworkModel networkModel = getNetworkModel();
            UpfTelcoNetworkModel.NetworkTransferResult transferResult = null;
            
            if (networkModel != null) {
                ComputingNode tsoNode = findTsoNode();
                transferResult = networkModel.calculateNetworkTransferWithDetails(dataTask.getEdgeDevice(), tsoNode, dataTask, dataSize);
            }
            
            // **Calculate REAL arrival time with 3-hop network delay (PMU → GNB → TELCO → GNB)**
            double networkDelay = transferResult != null ? transferResult.totalDelay : calculateBasicNetworkDelay(dataSize);
            double realArrivalTime = generationTime + networkDelay;
            
            // **Get path string for logging (3-hop path)**
            String path = transferResult != null ? transferResult.getPathString() : 
                         String.format("UpfTelco_%d -> GNB_? -> TELCO -> GNB_? (basic calculation)", upfTelcoId);
            
            // **Buffer tasks for batch processing - Collection window starts when data arrives at GNB after TELCO round trip**
            if (generationTimeBuffer.get(generationTime) == null) {
                generationTimeBuffer.put(generationTime, new ArrayList<>());
                
                // **FIXED: Use higher precision for fractional generation times**
                int timeoutId = (int)(generationTime * 1000); // Convert to milliseconds for precision
                schedule(this, MAX_WAITING_LATENCY, GENERATION_TIME_TIMEOUT, timeoutId);
                
                System.out.printf("UpfTelcoDataCollectorDynamic - GNB_%s started collection window for generation time %.3f after 3-hop path (timeout in %.3fs)%n", 
                                 gnbId, generationTime, MAX_WAITING_LATENCY);
            }
            
            // Add task to buffer with detailed information (3-hop path)
            TaskWithArrivalTime taskWithTime = new TaskWithArrivalTime(dataTask, realArrivalTime, upfTelcoId, networkDelay, path);
            generationTimeBuffer.get(generationTime).add(taskWithTime);
            
            // Log arrival with 3-hop information (debug only)
            System.out.printf("UpfTelcoDataCollectorDynamic - GNB_%s received UpfTelco_%d data after 3-hop path (gen=%.3f, arrival=%.3f, total_delay=%.3fs)%n", 
                             gnbId, upfTelcoId, generationTime, realArrivalTime, networkDelay);
        }
    }
    
    /**
     * Task with arrival time tracking for dynamic windows
     */
    private static class TaskWithArrivalTime {
        public final Task task;
        public final double realArrivalTime;
        public final int upfTelcoId;
        public final double networkDelay;
        public final String path;
        
        public TaskWithArrivalTime(Task task, double realArrivalTime, int upfTelcoId, double networkDelay, String path) {
            this.task = task;
            this.realArrivalTime = realArrivalTime;
            this.upfTelcoId = upfTelcoId;
            this.networkDelay = networkDelay;
            this.path = path;
        }
    }
    
    /**
     * Processes buffered tasks and applies MAX_WAITING_LATENCY rule starting from first arrival
     */
    private void handleGenerationTimeTimeout(int batchId) {
        synchronized(dataCollectionLock) {
            // Find the generation time for this batch ID
            double targetGenerationTime = batchId / 1000.0;
            
            List<TaskWithArrivalTime> tasks = generationTimeBuffer.get(targetGenerationTime);
            if (tasks == null || tasks.isEmpty()) {
                return;
            }
            
            // Create a copy to avoid concurrent modification
            tasks = new ArrayList<>(tasks);
            
            System.out.printf("UpfTelcoDataCollectorDynamic - GNB_%s: Processing Generation Time %.3f with %d tasks%n", 
                             gnbId, targetGenerationTime, tasks.size());
            
            // **Sort tasks by real arrival time (when they arrive at GNB after 3-hop path)**
            tasks.sort((t1, t2) -> Double.compare(t1.realArrivalTime, t2.realArrivalTime));
            
            // **COLLECTION WINDOW STARTS from first arrival at GNB (after 3-hop path)**
            double firstArrivalTime = tasks.get(0).realArrivalTime;
            double deadline = firstArrivalTime + MAX_WAITING_LATENCY;
            
            System.out.printf("UpfTelcoDataCollectorDynamic - GNB_%s: Collection window starts at %.4fs, deadline %.4fs%n", 
                             gnbId, firstArrivalTime, deadline);
            
            // **Apply MAX_WAITING_LATENCY filter**
            List<TaskWithArrivalTime> onTimeTasks = new ArrayList<>();
            int droppedTasks = 0;
            
            for (TaskWithArrivalTime taskWithTime : tasks) {
                if (taskWithTime.realArrivalTime <= deadline) {
                    onTimeTasks.add(taskWithTime);
                    
                    // **Log on-time data transfer**
                    UpfTelcoLogger.getInstance().logUpfTelcoDataTransferFull(
                        taskWithTime.task, taskWithTime.upfTelcoId, UPFTELCO_DATA_SIZE_KB, 
                        taskWithTime.path, false, taskWithTime.networkDelay);
                    
                    // **NEW: Track network volume for each hop in 3-hop path**
                    UpfTelcoLogger.getInstance().trackNetworkVolume("PMU_to_GNB", UPFTELCO_DATA_SIZE_KB);
                    UpfTelcoLogger.getInstance().trackNetworkVolume("GNB_to_TELCO", UPFTELCO_DATA_SIZE_KB);
                    UpfTelcoLogger.getInstance().trackNetworkVolume("TELCO_to_GNB", UPFTELCO_DATA_SIZE_KB);
                    
                    // **NEW: Track individual PMU and GNB data volumes**
                    String pmuId = "PMU_" + taskWithTime.upfTelcoId;
                    String currentGnbId = "GNB_" + gnbId;
                    UpfTelcoLogger.getInstance().trackPmuDataGeneration(pmuId, UPFTELCO_DATA_SIZE_KB);
                    UpfTelcoLogger.getInstance().trackGnbDataArrival(currentGnbId, UPFTELCO_DATA_SIZE_KB);
                    UpfTelcoLogger.getInstance().trackTelcoDataArrival(UPFTELCO_DATA_SIZE_KB);
                    
                    System.out.printf("UpfTelcoDataCollectorDynamic - GNB_%s: ON-TIME PMU_%d at %.4f (deadline: %.4f, margin: %.4fs)%n", 
                                     gnbId, taskWithTime.upfTelcoId, taskWithTime.realArrivalTime, deadline, 
                                     deadline - taskWithTime.realArrivalTime);
                } else {
                    droppedTasks++;
                    droppedLateArrivals++;
                    
                    // **Log late data transfer**
                    UpfTelcoLogger.getInstance().logUpfTelcoDataTransferFull(
                        taskWithTime.task, taskWithTime.upfTelcoId, UPFTELCO_DATA_SIZE_KB, 
                        taskWithTime.path, true, taskWithTime.networkDelay);
                    
                    // **NEW: Track network volume even for late arrivals (data still transmitted)**
                    UpfTelcoLogger.getInstance().trackNetworkVolume("PMU_to_GNB", UPFTELCO_DATA_SIZE_KB);
                    UpfTelcoLogger.getInstance().trackNetworkVolume("GNB_to_TELCO", UPFTELCO_DATA_SIZE_KB);
                    UpfTelcoLogger.getInstance().trackNetworkVolume("TELCO_to_GNB", UPFTELCO_DATA_SIZE_KB);
                    
                    // **NEW: Track individual PMU and GNB data volumes (even for late arrivals)**
                    String pmuId = "PMU_" + taskWithTime.upfTelcoId;
                    String currentGnbId = "GNB_" + gnbId;
                    UpfTelcoLogger.getInstance().trackPmuDataGeneration(pmuId, UPFTELCO_DATA_SIZE_KB);
                    UpfTelcoLogger.getInstance().trackGnbDataArrival(currentGnbId, UPFTELCO_DATA_SIZE_KB);
                    UpfTelcoLogger.getInstance().trackTelcoDataArrival(UPFTELCO_DATA_SIZE_KB);
                    
                    System.out.printf("UpfTelcoDataCollectorDynamic - GNB_%s: LATE PMU_%d at %.4f (deadline: %.4f, late by: %.4fs) - REJECTED%n", 
                                     gnbId, taskWithTime.upfTelcoId, taskWithTime.realArrivalTime, deadline, 
                                     taskWithTime.realArrivalTime - deadline);
                }
            }
            
            System.out.printf("UpfTelcoDataCollectorDynamic - GNB_%s: Collection Summary: %d on-time, %d dropped (%.1f%% success)%n", 
                             gnbId, onTimeTasks.size(), droppedTasks, 
                             (onTimeTasks.size() + droppedTasks > 0) ? (onTimeTasks.size() * 100.0) / (onTimeTasks.size() + droppedTasks) : 0.0);
            
            // **Create Grid Analysis Task with on-time tasks only**
            boolean isComplete = (onTimeTasks.size() >= REQUIRED_UPFTELCO_COUNT);
            createGridAnalysisTaskFromBuffer(targetGenerationTime, onTimeTasks, isComplete, firstArrivalTime);
            
            // **Clean up buffer**
            generationTimeBuffer.remove(targetGenerationTime);
        }
    }
    
    /**
     * **UPDATED: Creates a grid analysis task from filtered on-time PMU data**
     */
    private void createGridAnalysisTaskFromBuffer(double generationTime, List<TaskWithArrivalTime> onTimeTasks, 
                                                boolean isComplete, double firstArrivalTime) {
        if (onTimeTasks.isEmpty()) {
            System.out.printf("UpfTelcoDataCollectorDynamic - GNB_%s: No on-time measurements for Generation Time %.3f%n", 
                             gnbId, generationTime);
            
            // **Log collection completion with 0 PMUs**
            UpfTelcoLogger.getInstance().logDataCollectionComplete(generationTime, 0, REQUIRED_UPFTELCO_COUNT, false);
            return;
        }
        
        totalBatches++;
        
        // Use first task as base for getting application info
        Task baseTask = onTimeTasks.get(0).task;
        
        // Calculate total data size
        double totalDataSize = onTimeTasks.size() * UPFTELCO_DATA_SIZE_BITS;
        double totalDataKB = totalDataSize / 8192.0;
        
        // **CALCULATE PDC WAITING TIME** 
        List<Double> arrivalTimes = onTimeTasks.stream()
                                              .map(t -> t.realArrivalTime)
                                              .sorted()
                                              .collect(java.util.stream.Collectors.toList());
        
        double pdcWaitingTime = 0.0;
        
        if (onTimeTasks.size() >= REQUIRED_UPFTELCO_COUNT) {
            // **ALL PMUs ARRIVED ON TIME - PDC finished early**
            // PDC Waiting = time from first to last arrival
            if (arrivalTimes.size() > 1) {
                pdcWaitingTime = arrivalTimes.get(arrivalTimes.size() - 1) - arrivalTimes.get(0);
                
                System.out.printf("UpfTelcoDataCollectorDynamic - GNB_%s: ALL PMUs ON TIME: PDC Waiting Time: %.4fs (from %.4fs to %.4fs), Finished early!%n", 
                                 gnbId, pdcWaitingTime, arrivalTimes.get(0), arrivalTimes.get(arrivalTimes.size() - 1));
            }
            completeBatches++;
        } else {
            // **NOT ALL PMUs ARRIVED ON TIME - PDC waited full MAX_WAITING_LATENCY**
            // PDC Waiting = MAX_WAITING_LATENCY (PDC waited the full timeout period)
            pdcWaitingTime = MAX_WAITING_LATENCY;
            
            System.out.printf("UpfTelcoDataCollectorDynamic - GNB_%s: INCOMPLETE ARRIVAL: PDC Waiting Time: %.4fs (waited full timeout), Got %d/%d PMU devices%n", 
                             gnbId, pdcWaitingTime, onTimeTasks.size(), REQUIRED_UPFTELCO_COUNT);
            timeoutBatches++;
        }
        
        // **VERIFY PDC waiting time logic**
        if (onTimeTasks.size() >= REQUIRED_UPFTELCO_COUNT && pdcWaitingTime > MAX_WAITING_LATENCY) {
            System.err.printf("ERROR: PDC Waiting Time %.4fs exceeds MAX_WAITING_LATENCY %.4fs even with all PMU devices!%n", 
                             pdcWaitingTime, MAX_WAITING_LATENCY);
        }
        
        // Create GNB-specific task description
        String batchType = isComplete ? "COMPLETE" : "TIMEOUT";
        String taskDescription = String.format("GridAnalysisTask_%s:Window_%.3f:%d/%d:%s", 
                                             gnbId, generationTime, onTimeTasks.size(), REQUIRED_UPFTELCO_COUNT, batchType);
        
        // **Log data collection completion with correct PMU counts**
        UpfTelcoLogger.getInstance().logDataCollectionComplete(generationTime, onTimeTasks.size(), REQUIRED_UPFTELCO_COUNT, isComplete);
        
        // Create new analysis task (following same pattern as EdgeDataCollectorDynamic)
        Task analysisTask;
        try {
            int newTaskId = (int)(baseTask.getId() + 10000 + (generationTime * 1000));
            analysisTask = new com.mechalikh.pureedgesim.taskgenerator.DefaultTask(newTaskId);
            
            // **SET Grid Analysis specific properties**
            analysisTask.setType(taskDescription);
            
            // **GNB-specific collector: execute locally on this GNB**
            analysisTask.setOffloadingDestination(assignedGnb);
            analysisTask.setEdgeDevice(assignedGnb);
            analysisTask.setOrchestrator(assignedGnb);
            
            analysisTask.setRegistry(baseTask.getRegistry());
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
            
            // **LOG grid analysis task creation with PMU timing**
            UpfTelcoLogger.getInstance().logStateEstimationTaskWithTiming(
                newTaskId, generationTime, onTimeTasks.size(), REQUIRED_UPFTELCO_COUNT, 
                batchType, totalDataKB, pdcWaitingTime
            );
            
            // **Store first data network delay for custom total time calculation**
            double firstDataNetworkDelay = onTimeTasks.get(0).networkDelay;
            UpfTelcoLogger.getInstance().storeFirstDataNetworkDelay(newTaskId, firstDataNetworkDelay);
            
            // **NEW: Track State Estimation task data volume**
            UpfTelcoLogger.getInstance().trackNetworkVolume("StateEstimation_Input", totalDataKB);
            UpfTelcoLogger.getInstance().trackNetworkVolume("StateEstimation_Output", GRID_ANALYSIS_OUTPUT_SIZE_KB);
            
            // **NEW: Track TSO data arrival (Grid Analysis results go to TSO)**
            UpfTelcoLogger.getInstance().trackTsoDataArrival(GRID_ANALYSIS_OUTPUT_SIZE_KB);
            
            // **EXECUTE the grid analysis task immediately**
            scheduleNow(simulationManager, com.mechalikh.pureedgesim.simulationmanager.SimulationManager.SEND_TO_ORCH, analysisTask);
            
            System.out.printf("UpfTelcoDataCollectorDynamic - GNB_%s created GridAnalysisTask_%s_%d for Generation Time %.3f (%d/%d PMUs, %.4fs PDC wait, %.2f KB data)%n",
                             gnbId, gnbId, newTaskId, generationTime, onTimeTasks.size(), REQUIRED_UPFTELCO_COUNT, pdcWaitingTime, totalDataKB);
            
        } catch (Exception e) {
            System.err.println("UpfTelcoDataCollectorDynamic - GNB_" + gnbId + ": Error creating grid analysis task: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Extract UpfTelco ID from device name or find device index
     */
    private int extractUpfTelcoId(ComputingNode upfTelcoDevice, Task dataTask) {
        try {
            String deviceName = upfTelcoDevice.getName();
            if (deviceName != null && deviceName.startsWith("PMU_")) {
                return Integer.parseInt(deviceName.substring(4));
            }
        } catch (NumberFormatException e) {
            // Fall back to finding index
        }
        
        // Try extracting from task ID
        try {
            return extractUpfTelcoIdFromTask(dataTask);
        } catch (Exception e) {
            // Continue to device list fallback
        }
        
        // Fallback: find device index in the device list
        try {
            List<ComputingNode> devices = simulationManager.getDataCentersManager()
                    .getComputingNodesGenerator().getMistOnlyList();
            
            for (int i = 0; i < devices.size(); i++) {
                if (devices.get(i).equals(upfTelcoDevice)) {
                    return i; // UpfTelco IDs start from 0
                }
            }
        } catch (Exception e) {
            System.err.println("UpfTelcoDataCollectorDynamic - Error finding UpfTelco device index: " + e.getMessage());
        }
        
        return 0; // Unknown UpfTelco
    }
    
    /**
     * Extract UpfTelco ID from task ID patterns
     */
    private int extractUpfTelcoIdFromTask(Task dataTask) {
        long taskId = dataTask.getId();
        
        // For task IDs < 10000, assume they are PMU data tasks
        // and calculate PMU ID based on timing patterns
        if (taskId < 10000) {
            // Pattern analysis: tasks are generated in groups by time
            // Find corresponding PMU based on task timing and generation order
            int totalPmus = simulationManager.getDataCentersManager()
                    .getComputingNodesGenerator().getMistOnlyList().size();
            
            if (totalPmus > 0) {
                return (int)(taskId % totalPmus);
            }
        }
        
        return 0; // Default fallback
    }
    
    /**
     * Get network model for transfer calculations
     */
    private UpfTelcoNetworkModel getNetworkModel() {
        try {
            if (simulationManager.getNetworkModel() instanceof UpfTelcoNetworkModel) {
                return (UpfTelcoNetworkModel) simulationManager.getNetworkModel();
            }
        } catch (Exception e) {
            System.err.println("UpfTelcoDataCollectorDynamic - Error getting network model: " + e.getMessage());
        }
        return null;
    }
    
    /**
     * Calculate basic network delay if detailed calculation fails
     */
    private double calculateBasicNetworkDelay(double dataSize) {
        // Basic delay calculation: transmission time + latency
        double bandwidth = SimulationParameters.cellularBandwidthBitsPerSecond; // UpfTelco to GNB
        double latency = SimulationParameters.cellularLatency;
        
        double transmissionTime = dataSize / bandwidth;
        double totalDelay = transmissionTime + latency;
        
        // Add some jitter (±10%)
        double jitter = (Math.random() - 0.5) * 0.2 * totalDelay;
        return Math.max(0.001, totalDelay + jitter); // Minimum 1ms
    }
    
    /**
     * Find TSO node (cloud node) for compatibility
     */
    private ComputingNode findTsoNode() {
        try {
            List<ComputingNode> cloudNodes = simulationManager.getDataCentersManager()
                    .getComputingNodesGenerator().getCloudOnlyList();
            
            if (!cloudNodes.isEmpty()) {
                return cloudNodes.get(0); // Return first cloud node as TSO
            }
        } catch (Exception e) {
            System.err.println("UpfTelcoDataCollectorDynamic - Error finding TSO node: " + e.getMessage());
        }
        
        return null;
    }
    
    /**
     * Get statistics for this data collector
     */
    public String getStatistics() {
        return String.format("GNB_%s: %d batches (%d complete, %d timeout), %d dropped late arrivals", 
                           gnbId, totalBatches, completeBatches, timeoutBatches, droppedLateArrivals);
    }
    
    /**
     * Extract GNB ID from name
     */
    private String extractGnbId(String gnbName) {
        if (gnbName != null && gnbName.startsWith("EDGE_")) {
            try {
                return gnbName.substring(5); // Extract number part
            } catch (Exception e) {
                System.err.println("UpfTelcoDataCollectorDynamic - Could not extract GNB ID from: " + gnbName);
            }
        }
        
        return "Unknown";
    }
    
    /**
     * Get total number of GNBs for PMU distribution calculation
     */
    private int getGnbCount() {
        try {
            List<ComputingNode> edgeDatacenters = simulationManager.getDataCentersManager()
                    .getComputingNodesGenerator().getEdgeOnlyList();
            
            // Count GNBs (exclude TELCO if it exists)
            int gnbCount = 0;
            for (ComputingNode edge : edgeDatacenters) {
                if (edge.getName() == null || !edge.getName().equals("TELCO")) {
                    gnbCount++;
                }
            }
            return gnbCount;
        } catch (Exception e) {
            return 1; // Fallback to 1 GNB
        }
    }
    
    /**
     * Check if a PMU is assigned to this GNB based on distance
     */
    private boolean isPmuAssignedToThisGnb(ComputingNode pmuDevice) {
        if (pmuDevice == null || assignedGnb == null) {
            return false;
        }
        
        try {
            // Get all GNBs
            List<ComputingNode> edgeDatacenters = simulationManager.getDataCentersManager()
                    .getComputingNodesGenerator().getEdgeOnlyList();
            
            List<ComputingNode> gnbNodes = new ArrayList<>();
            for (ComputingNode edge : edgeDatacenters) {
                if (edge.getName() == null || !edge.getName().equals("TELCO")) {
                    gnbNodes.add(edge);
                }
            }
            
            // Find closest GNB to this PMU
            ComputingNode closestGnb = null;
            double minDistance = Double.MAX_VALUE;
            
            for (ComputingNode gnb : gnbNodes) {
                double distance = calculateEuclideanDistance(pmuDevice, gnb);
                if (distance < minDistance) {
                    minDistance = distance;
                    closestGnb = gnb;
                }
            }
            
            // Check if this GNB is the closest one
            return assignedGnb.equals(closestGnb);
            
        } catch (Exception e) {
            System.err.println("UpfTelcoDataCollectorDynamic - Error checking PMU assignment: " + e.getMessage());
            return false;
        }
    }
    
    /**
     * Calculate Euclidean distance between two nodes
     */
    private double calculateEuclideanDistance(ComputingNode node1, ComputingNode node2) {
        try {
            Location loc1 = node1.getMobilityModel().getCurrentLocation();
            Location loc2 = node2.getMobilityModel().getCurrentLocation();
            
            double dx = loc1.getXPos() - loc2.getXPos();
            double dy = loc1.getYPos() - loc2.getYPos();
            
            return Math.sqrt(dx * dx + dy * dy);
        } catch (Exception e) {
            return Double.MAX_VALUE;
        }
    }
}