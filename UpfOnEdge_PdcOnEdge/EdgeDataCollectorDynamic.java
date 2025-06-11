package UpfOnEdge_PdcOnEdge;

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
public class EdgeDataCollectorDynamic extends SimEntity {
    
    // Event tags
    public static final int EDGE_DATA_RECEIVED = 300;
    public static final int GENERATION_TIME_TIMEOUT = 301;
    
    // Collection parameters  
    private final int REQUIRED_EDGE_COUNT; // PMUs assigned to this GNB
    private static final double MAX_WAITING_LATENCY =  0.045; // **max waiting for late Edge data**
    
    // **NEW: GNB-specific properties**
    private final ComputingNode assignedGnb; // The GNB this collector belongs to
    private final String gnbId; // GNB identifier (e.g., "1", "2", "3")
    
    // Fixed Edge data size (2KB)
    private static final double EDGE_DATA_SIZE_KB = 2.0;
    private static final double EDGE_DATA_SIZE_BITS = EDGE_DATA_SIZE_KB * 8192.0;
    
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
     * Edge Measurement data structure
     */
    public static class EdgeMeasurement {
        public final int edgeId;
        public final double arrivalTime;
        public final double generationTime; // **NEW: Task generation time**
        public final double dataSize;
        public final Task originalTask;
        
        public EdgeMeasurement(int edgeId, double arrivalTime, double generationTime, double dataSize, Task originalTask) {
            this.edgeId = edgeId;
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
        public final double firstArrivalTime; // When first Edge arrived (collection window start)
        public final double deadline; // When this batch will timeout
        public final Map<Integer, EdgeMeasurement> measurements = Collections.synchronizedMap(new HashMap<>());
        
        public ArrivalTimeBatch(int batchId, double firstArrivalTime, double maxWaitingLatency) {
            this.batchId = batchId;
            this.firstArrivalTime = firstArrivalTime;
            this.deadline = firstArrivalTime + maxWaitingLatency;
        }
    }
    
    public EdgeDataCollectorDynamic(SimulationManager simulationManager, ComputingNode assignedGnb) {
        super(simulationManager.getSimulation());
        this.simulationManager = simulationManager;
        this.assignedGnb = assignedGnb;
        this.tsoNode = null; // No longer needed for per-GNB processing
        
        // Extract GNB ID from name (e.g., "EDGE_1" -> "1")
        this.gnbId = extractGnbId(assignedGnb.getName());
        
        // Calculate PMUs assigned to this GNB (approximately equal distribution)
        int totalPmus = SimulationParameters.maxNumberOfEdgeDevices;
        int totalGnbs = getGnbCount();
        this.REQUIRED_EDGE_COUNT = (int) Math.ceil((double) totalPmus / totalGnbs);
        
        System.out.printf("EdgeDataCollectorDynamic - Initialized for GNB_%s expecting ~%d PMUs (of %d total PMUs)%n", 
                         gnbId, REQUIRED_EDGE_COUNT, totalPmus);
    }
    
    /**
     * Constructor with explicit PMU count (preferred for accurate expectations)
     */
    public EdgeDataCollectorDynamic(SimulationManager simulationManager, ComputingNode assignedGnb, int expectedPmuCount) {
        super(simulationManager.getSimulation());
        this.simulationManager = simulationManager;
        this.assignedGnb = assignedGnb;
        this.tsoNode = null; // No longer needed for per-GNB processing
        
        // Extract GNB ID from name (e.g., "EDGE_1" -> "1")
        this.gnbId = extractGnbId(assignedGnb.getName());
        
        // Use the exact PMU count assigned to this GNB
        this.REQUIRED_EDGE_COUNT = expectedPmuCount;
        
        System.out.printf("EdgeDataCollectorDynamic - Initialized for GNB_%s expecting %d assigned PMUs%n", 
                         gnbId, REQUIRED_EDGE_COUNT);
    }
    

    
    @Override
    public void processEvent(Event ev) {
        // Handle events with proper synchronization
        switch (ev.getTag()) {
            case EDGE_DATA_RECEIVED:
                collectEdgeData((Task) ev.getData());
                break;
            case GENERATION_TIME_TIMEOUT:
                handleGenerationTimeTimeout((Integer) ev.getData());
                break;
            default:
                System.err.println("EdgeDataCollectorDynamic - Unknown event: " + ev.getTag());
        }
    }
    
    /**
     * **NEW: GNB-specific data collection with PMU filtering**
     */
    public void collectEdgeData(Task dataTask) {
        synchronized(dataCollectionLock) {
            double currentTime = simulationManager.getSimulation().clock();
            double generationTime = dataTask.getTime(); // Task generation time
            
            // Extract Edge information
            int edgeId = extractEdgeId(dataTask.getEdgeDevice(), dataTask);
            
            // **FILTER: Only accept PMU data intended for this GNB**
            if (!isPmuAssignedToThisGnb(dataTask.getEdgeDevice())) {
                System.out.printf("EdgeDataCollectorDynamic - GNB_%s rejected PMU data from Edge_%d (not assigned to this GNB)%n", 
                                 gnbId, edgeId);
                return;
            }
            
            // **Use fixed 2KB data size**
            double dataSize = EDGE_DATA_SIZE_BITS;
            double dataSizeKB = EDGE_DATA_SIZE_KB;
            
            // Override the task's file size to ensure consistency
            dataTask.setFileSizeInBits((long)dataSize);
            
            // **Get network transfer details from EdgeNetworkModel (consistent with actual transfer)**
            EdgeNetworkModel networkModel = getNetworkModel();
            EdgeNetworkModel.NetworkTransferResult transferResult = null;
            
            if (networkModel != null) {
                ComputingNode tsoNode = findTsoNode();
                transferResult = networkModel.calculateNetworkTransferWithDetails(dataTask.getEdgeDevice(), tsoNode, dataTask, dataSize);
            }
            
            // **Calculate REAL arrival time with network delay**
            double networkDelay = transferResult != null ? transferResult.totalDelay : calculateBasicNetworkDelay(dataSize);
            double realArrivalTime = generationTime + networkDelay;
            
            // **Get path string for logging**
            String path = transferResult != null ? transferResult.getPathString() : 
                         String.format("Edge_%d -> GNB_? -> TELCO -> TSO (basic calculation)", edgeId);
            
            // **Buffer tasks for batch processing**
            if (generationTimeBuffer.get(generationTime) == null) {
                generationTimeBuffer.put(generationTime, new ArrayList<>());
                
                // **FIXED: Use higher precision for fractional generation times**
                // Convert generation time to integer with 1000x precision (0.333 -> 333, 0.667 -> 667)
                int batchId = (int)Math.round(generationTime * 1000);
                
                // **Schedule processing after a short delay to collect all tasks of this generation time**
                // Use fixed delay to ensure predictable scheduling
                schedule(this, 0.001, GENERATION_TIME_TIMEOUT, batchId);
            }
            
            // **Add task to buffer along with its real arrival time**
            TaskWithArrivalTime taskWithTime = new TaskWithArrivalTime(dataTask, realArrivalTime, edgeId, networkDelay, path);
            generationTimeBuffer.get(generationTime).add(taskWithTime);
            
            System.out.printf("EdgeDataCollectorDynamic - GNB_%s: Buffered PMU_%d data (gen=%.3f) - Buffer size: %d%n", 
                             gnbId, edgeId, generationTime, generationTimeBuffer.get(generationTime).size());
        }
    }
    
    /**
     * Helper class to store task with its arrival time info
     */
    private static class TaskWithArrivalTime {
        public final Task task;
        public final double realArrivalTime;
        public final int edgeId;
        public final double networkDelay;
        public final String path;
        
        public TaskWithArrivalTime(Task task, double realArrivalTime, int edgeId, double networkDelay, String path) {
            this.task = task;
            this.realArrivalTime = realArrivalTime;
            this.edgeId = edgeId;
            this.networkDelay = networkDelay;
            this.path = path;
        }
    }
    
    /**
     * Processes buffered tasks and applies MAX_WAITING_LATENCY rule
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
            
            System.out.printf("EdgeDataCollectorDynamic - Processing Generation Time %.3f with %d tasks%n", 
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
                    EdgeLogger.getInstance().logEdgeDataTransferFull(
                        taskWithTime.task, taskWithTime.edgeId, EDGE_DATA_SIZE_KB, 
                        taskWithTime.path, false, taskWithTime.networkDelay);
                    
                    System.out.printf("EdgeDataCollectorDynamic - ON-TIME: Edge %d at %.4f (deadline: %.4f, margin: %.4fs)%n", 
                                     taskWithTime.edgeId, taskWithTime.realArrivalTime, deadline, 
                                     deadline - taskWithTime.realArrivalTime);
                } else {
                    droppedTasks++;
                    droppedLateArrivals++;
                    
                    // **Log late data transfer**
                    EdgeLogger.getInstance().logEdgeDataTransferFull(
                        taskWithTime.task, taskWithTime.edgeId, EDGE_DATA_SIZE_KB, 
                        taskWithTime.path, true, taskWithTime.networkDelay);
                    
                    System.out.printf("EdgeDataCollectorDynamic - LATE ARRIVAL: Edge %d at %.4f (deadline: %.4f, late by: %.4fs) - REJECTED%n", 
                                     taskWithTime.edgeId, taskWithTime.realArrivalTime, deadline, 
                                     taskWithTime.realArrivalTime - deadline);
                }
            }
            
            System.out.printf("EdgeDataCollectorDynamic - Collection Window Summary: %d on-time, %d dropped (%.1f%% success)%n", 
                             onTimeTasks.size(), droppedTasks, 
                             (onTimeTasks.size() * 100.0) / (onTimeTasks.size() + droppedTasks));
            
            // **Create State Estimation Task with on-time tasks only**
            boolean isComplete = (onTimeTasks.size() >= REQUIRED_EDGE_COUNT);
            createStateEstimationTaskFromBuffer(targetGenerationTime, onTimeTasks, isComplete, firstArrivalTime);
            
            // **Clean up buffer**
            generationTimeBuffer.remove(targetGenerationTime);
        }
    }
    
    /**
     * Creates a state estimation task from filtered on-time Edge data
     */
    private void createStateEstimationTaskFromBuffer(double generationTime, List<TaskWithArrivalTime> onTimeTasks, 
                                                   boolean isComplete, double firstArrivalTime) {
        if (onTimeTasks.isEmpty()) {
            System.out.printf("EdgeDataCollectorDynamic - No on-time measurements for Generation Time %.3f%n", generationTime);
            return;
        }
        
        totalBatches++;
        
        // Use first task as base for getting application info
        Task baseTask = onTimeTasks.get(0).task;
        
        // Calculate total data size
        double totalDataSize = onTimeTasks.size() * EDGE_DATA_SIZE_BITS;
        double totalDataKB = totalDataSize / 8192.0;
        
        // **CALCULATE PDC WAITING TIME** 
        List<Double> arrivalTimes = onTimeTasks.stream()
                                              .map(t -> t.realArrivalTime)
                                              .sorted()
                                              .collect(java.util.stream.Collectors.toList());
        
        double pdcWaitingTime = 0.0;
        
        if (onTimeTasks.size() >= REQUIRED_EDGE_COUNT) {
            // **ALL Edge ARRIVED ON TIME - PDC finished early**
            // PDC Waiting = time from first to last arrival
            if (arrivalTimes.size() > 1) {
                pdcWaitingTime = arrivalTimes.get(arrivalTimes.size() - 1) - arrivalTimes.get(0);
                
                System.out.printf("EdgeDataCollectorDynamic - ALL Edge ON TIME: PDC Waiting Time: %.4fs (from %.4fs to %.4fs), Finished early!%n", 
                                 pdcWaitingTime, arrivalTimes.get(0), arrivalTimes.get(arrivalTimes.size() - 1));
            }
        } else {
            // **NOT ALL Edge ARRIVED ON TIME - PDC waited full MAX_WAITING_LATENCY**
            // PDC Waiting = MAX_WAITING_LATENCY (PDC waited the full timeout period)
            pdcWaitingTime = MAX_WAITING_LATENCY;
            
            System.out.printf("EdgeDataCollectorDynamic - INCOMPLETE ARRIVAL: PDC Waiting Time: %.4fs (waited full timeout), Got %d/%d Edge devices%n", 
                             pdcWaitingTime, onTimeTasks.size(), REQUIRED_EDGE_COUNT);
        }
        
        // **VERIFY PDC waiting time logic**
        if (onTimeTasks.size() >= REQUIRED_EDGE_COUNT && pdcWaitingTime > MAX_WAITING_LATENCY) {
            System.err.printf("ERROR: PDC Waiting Time %.4fs exceeds MAX_WAITING_LATENCY %.4fs even with all Edge devices!%n", 
                             pdcWaitingTime, MAX_WAITING_LATENCY);
        }
        
        // Create GNB-specific task description
        String batchType = isComplete ? "COMPLETE" : "TIMEOUT";
        String taskDescription = String.format("GridAnalysisTask_%s:Window_%.3f:%d/%d:%s", 
                                             gnbId, generationTime, onTimeTasks.size(), REQUIRED_EDGE_COUNT, batchType);
        
        // Create new analysis task
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
            analysisTask.setFileSizeInBits((long)totalDataSize); // Input is filtered Edge data
            analysisTask.setOutputSizeInBits(GRID_ANALYSIS_OUTPUT_SIZE_KB * 8192); // 50KB analysis result
            
            // **LOG grid analysis task creation**
            EdgeLogger.getInstance().logStateEstimationTaskCreation(
                analysisTask, (int)(generationTime * 1000), onTimeTasks.size(), REQUIRED_EDGE_COUNT, 
                pdcWaitingTime, totalDataKB, batchType
            );
            
            // Log data collection completion
            EdgeLogger.getInstance().logDataCollectionComplete(generationTime, onTimeTasks.size(), isComplete);
            
            // **EXECUTE the grid analysis task immediately**
            scheduleNow(simulationManager, com.mechalikh.pureedgesim.simulationmanager.SimulationManager.SEND_TO_ORCH, analysisTask);
            
            System.out.printf("EdgeDataCollectorDynamic - GNB_%s created GridAnalysisTask_%s_%d for Generation Time %.3f (%d/%d PMUs, %.4fs PDC wait, %.2f KB data)%n",
                             gnbId, gnbId, newTaskId, generationTime, onTimeTasks.size(), REQUIRED_EDGE_COUNT, pdcWaitingTime, totalDataKB);
            
            if (isComplete) {
                completeBatches++;
            } else {
                timeoutBatches++;
            }
            
        } catch (Exception e) {
            System.err.println("EdgeDataCollectorDynamic - Error creating grid analysis task: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Extract Edge ID from edge device or task
     */
    private int extractEdgeId(ComputingNode edgeDevice, Task dataTask) {
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
                List<ComputingNode> edgeDevices = simulationManager.getDataCentersManager()
                        .getComputingNodesGenerator().getMistOnlyList();
                for (int i = 0; i < edgeDevices.size(); i++) {
                    if (edgeDevices.get(i).equals(edgeDevice)) {
                        return i;
                    }
                }
            } catch (Exception e) {
                System.err.println("EdgeDataCollectorDynamic - Error finding Edge ID from device list: " + e.getMessage());
            }
        }
        
        // Final fallback: extract from task ID
        return extractEdgeIdFromTask(dataTask);
    }
    
    /**
     * Extract Edge ID from task properties
     */
    private int extractEdgeIdFromTask(Task dataTask) {
        try {
            // Use task ID modulo to distribute across available Edge range
            return (int)(dataTask.getId() % REQUIRED_EDGE_COUNT);
        } catch (Exception e) {
            System.err.println("EdgeDataCollectorDynamic - Error extracting Edge ID from task: " + e.getMessage());
            return 0; // Default fallback
        }
    }
    
    /**
     * Get the EdgeNetworkModel instance from simulation manager
     */
    private EdgeNetworkModel getNetworkModel() {
        try {
            // Get the network model from simulation manager
            if (simulationManager.getNetworkModel() instanceof EdgeNetworkModel) {
                return (EdgeNetworkModel) simulationManager.getNetworkModel();
            }
        } catch (Exception e) {
            System.err.println("EdgeDataCollectorDynamic - Could not get EdgeNetworkModel: " + e.getMessage());
        }
        return null;
    }
    
    /**
     * Basic network delay calculation as fallback
     */
    private double calculateBasicNetworkDelay(double dataSize) {
        // Basic calculation: transfer time + latency + minimal jitter
        double edgeToGnbTime = dataSize / SimulationParameters.cellularBandwidthBitsPerSecond + 
                             SimulationParameters.cellularLatency + (random.nextDouble() * 0.005);
        
        double gnbToTelcoTime = dataSize / SimulationParameters.manBandwidthBitsPerSecond + 
                               SimulationParameters.manLatency + (random.nextDouble() * 0.002);
        
        double telcoToTsoTime = dataSize / SimulationParameters.wanBandwidthBitsPerSecond + 
                               SimulationParameters.wanLatency + (random.nextDouble() * 0.008);
        
        return edgeToGnbTime + gnbToTelcoTime + telcoToTsoTime;
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
        return String.format("GNB_%s Collection Windows: %d total, %d complete, %d timeout, %d late arrivals dropped",
                           gnbId, totalBatches, completeBatches, timeoutBatches, droppedLateArrivals);
    }
    
    /**
     * Extract GNB ID from GNB name
     */
    private String extractGnbId(String gnbName) {
        if (gnbName != null) {
            if (gnbName.startsWith("EDGE_")) {
                return gnbName.substring(5); // Remove "EDGE_" prefix
            } else if (gnbName.startsWith("Edge_")) {
                return gnbName.substring(5); // Remove "Edge_" prefix
            }
            // Try to extract number using regex
            java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("\\d+");
            java.util.regex.Matcher matcher = pattern.matcher(gnbName);
            if (matcher.find()) {
                return matcher.group();
            }
        }
        return "Unknown"; // Fallback
    }
    
    /**
     * Get total number of GNBs in the simulation
     */
    private int getGnbCount() {
        try {
            List<ComputingNode> edgeDatacenters = simulationManager.getDataCentersManager()
                                                                  .getComputingNodesGenerator()
                                                                  .getEdgeOnlyList();
            
            // Count GNBs (exclude TELCO if it exists)
            int gnbCount = 0;
            for (ComputingNode edge : edgeDatacenters) {
                if (edge.getName() == null || !edge.getName().equals("TELCO")) {
                    gnbCount++;
                }
            }
            return Math.max(1, gnbCount); // At least 1 GNB
        } catch (Exception e) {
            System.err.println("EdgeDataCollectorDynamic - Error counting GNBs: " + e.getMessage());
            return 1; // Fallback
        }
    }
    
    /**
     * Check if a PMU is assigned to this GNB based on closest distance
     */
    private boolean isPmuAssignedToThisGnb(ComputingNode pmuDevice) {
        if (pmuDevice == null || assignedGnb == null) {
            return false;
        }
        
        try {
            // Find the closest GNB for this PMU
            List<ComputingNode> edgeDatacenters = simulationManager.getDataCentersManager()
                                                                  .getComputingNodesGenerator()
                                                                  .getEdgeOnlyList();
            
            ComputingNode closestGnb = null;
            double minDistance = Double.MAX_VALUE;
            
            for (ComputingNode gnb : edgeDatacenters) {
                // Skip TELCO and other non-GNB nodes
                if (gnb.getName() != null && gnb.getName().equals("TELCO")) {
                    continue;
                }
                
                double distance = calculateEuclideanDistance(pmuDevice, gnb);
                if (distance < minDistance) {
                    minDistance = distance;
                    closestGnb = gnb;
                }
            }
            
            // Check if this GNB is the closest one for this PMU
            return closestGnb != null && closestGnb.equals(assignedGnb);
            
        } catch (Exception e) {
            System.err.println("EdgeDataCollectorDynamic - Error checking PMU assignment: " + e.getMessage());
            return false; // Safe fallback
        }
    }
    
    /**
     * Calculate Euclidean distance between two nodes
     */
    private double calculateEuclideanDistance(ComputingNode node1, ComputingNode node2) {
        if (node1 == null || node2 == null) {
            return Double.MAX_VALUE;
        }
        
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