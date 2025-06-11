package UpfOnTelco_PdcOnEdge;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.io.File;

import com.mechalikh.pureedgesim.datacentersmanager.ComputingNode;
import com.mechalikh.pureedgesim.locationmanager.Location;
import com.mechalikh.pureedgesim.scenariomanager.SimulationParameters;
import com.mechalikh.pureedgesim.simulationmanager.SimLog;
import com.mechalikh.pureedgesim.simulationmanager.SimulationManager;
import com.mechalikh.pureedgesim.taskgenerator.Task;

/**
 * UpfTelco Logger for Smart Grid simulation logging.
 * Uses the Singleton pattern to ensure a single instance throughout the system.
 * Logs detailed information about PMU sensor data transfers and distributed GNB grid analysis.
 * 
 * Architecture: PMU → GNB (local collection and grid analysis per GNB)
 */
public class UpfTelcoLogger {
    private static UpfTelcoLogger instance = null;
    private PrintWriter logWriter;
    private PrintWriter csvWriter;
    private SimpleDateFormat dateFormat;
    private String outputPath;
    private SimulationManager simulationManager;
    
    // Control printing to terminal
    private static boolean PRINT_TO_TERMINAL = false; 
    
    // Maps for storing information per UpfTelco sensor
    private Map<Integer, Location> upfTelcoLocations = new HashMap<>();
    
    // Map for position history per UpfTelco sensor
    private Map<Integer, List<Location>> positionHistoryMap = new HashMap<>();
    
    // Store task information per second
    private boolean upfTelcoDatacentersInfoPrinted = false;
    
    // **NEW: Dynamic UpfTelco count**
    private int totalUpfTelcoCount = 14; // Default fallback
    
    // Adding new fields for CSV logging
    private List<String> csvRecords = new ArrayList<>();
    private static final String CSV_HEADER = "Time,PmuID,PmuCoordinates,DataSize,Path,HopSum,Status";
    
    // **NEW: State Estimation CSV logging**
    private List<String> stateEstimationCsvRecords = new ArrayList<>();
    private static final String STATE_ESTIMATION_CSV_HEADER = "Time,TaskID,GNBID,Window,Coverage,BatchType,InputDataKB,OutputDataKB,MaxLatency,ComputationMI,WaitTime,ExecTime,NetTime,TotalTime,Status,PDCWaitingTime,SuccessFlag";
    
    // Track network hops and times for each task
    private Map<Integer, StringBuilder> taskHops;
    private Map<Integer, Double> hopStartTimes;
    
    // Store task hop times
    private Map<String, Double> hopTimes = new HashMap<>();
    
    // Store expected paths for tasks
    private Map<Integer, String> expectedPaths = new HashMap<>();
    
    // Store GNB names for tasks
    private Map<Integer, String> taskGnbNames = new HashMap<>();
    
    // Store hop times for each task
    private Map<Integer, TaskHopInfo> taskHopTimes = new HashMap<>();
    
    // **NEW: Store PDC waiting times for each task**
    private Map<Integer, Double> taskCollectionTimes = new HashMap<>();
    
    // **NEW: Store first data network delays for each task**
    private Map<Integer, Double> taskFirstDataNetworkDelays = new HashMap<>();
    
    // **NEW: Network volume tracking per layer**
    private Map<String, Double> networkVolumePerLayer = new HashMap<>();
    private Map<String, Integer> transferCountPerLayer = new HashMap<>();
    private List<String> networkUsageCsvRecords = new ArrayList<>();
    private static final String NETWORK_USAGE_CSV_HEADER = "NetworkLevel,TotalDataVolumeKB,TransferCount,AverageDataSizeKB";
    
    /**
     * **Updated: Stores hop times for 3-hop path: PMU → GNB → TELCO → GNB**
     */
    public static class TaskHopInfo {
        public final String gnbName;
        public final double pmuToGnbTime;
        public final double gnbToTelcoTime; // NEW: GNB → TELCO time
        public final double telcoToGnbTime; // NEW: TELCO → GNB return time
        
        public TaskHopInfo(String gnbName, double pmuToGnbTime, double gnbToTelcoTime, double telcoToGnbTime) {
            this.gnbName = gnbName;
            this.pmuToGnbTime = pmuToGnbTime;
            this.gnbToTelcoTime = gnbToTelcoTime;
            this.telcoToGnbTime = telcoToGnbTime;
        }
    }
    
    private UpfTelcoLogger(String path) throws IOException {
        this.outputPath = path;
        this.dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        this.taskHops = new HashMap<>();
        this.hopStartTimes = new HashMap<>();
        
        // Create log files
        String timestamp = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date());
        String logFile = outputPath + "/" + timestamp + "/pmu_simulation.log";
        
        // Ensure directories exist
        new File(outputPath + "/" + timestamp).mkdirs();
        
        // Initialize writers
        this.logWriter = new PrintWriter(new FileWriter(logFile, true));
        // Removed csvWriter - we don't need task_completion.csv anymore
    }
    
    public static UpfTelcoLogger initialize(SimulationManager simulationManager, String outputPath) throws IOException {
        if (instance == null) {
            instance = new UpfTelcoLogger(outputPath);
        }
        instance.simulationManager = simulationManager;
        
        // **NEW: Read dynamic UpfTelco count from simulation parameters**
        instance.totalUpfTelcoCount = SimulationParameters.maxNumberOfEdgeDevices;
        
        return instance;
    }
    
    public static UpfTelcoLogger initialize(String outputPath) throws IOException {
        if (instance == null) {
            instance = new UpfTelcoLogger(outputPath);
        }
        return instance;
    }
    
    public static UpfTelcoLogger getInstance() {
        if (instance == null) {
            throw new IllegalStateException("UpfTelcoLogger has not been initialized. Call initialize() first.");
        }
        return instance;
    }
    
    /**
     * Enables or disables terminal printing
     */
    public static void setPrintToTerminal(boolean enabled) {
        PRINT_TO_TERMINAL = enabled;
    }
    
    /**
     * Returns the current status of terminal printing
     */
    public static boolean isPrintingToTerminal() {
        return PRINT_TO_TERMINAL;
    }
    
    /**
     * Prints information about upfTelco datacenters.
     */
    public void printUpfTelcoDatacentersInfo() {
        if (upfTelcoDatacentersInfoPrinted || simulationManager == null || 
            simulationManager.getDataCentersManager() == null) {
            return;
        }
        
        List<ComputingNode> upfTelcoDatacenters = simulationManager.getDataCentersManager()
                .getComputingNodesGenerator().getEdgeOnlyList();
        
        simulationManager.getSimulationLogger().printWithoutTime("===== UPFTELCO DATACENTERS =====");
        for (ComputingNode node : upfTelcoDatacenters) {
            simulationManager.getSimulationLogger().printWithoutTime(
                    "%s - Location: (%.1f, %.1f) - Resources: %.0f cores, %.0f MIPS, %.0f RAM, %.0f Storage",
                    node.getName(),
                    node.getMobilityModel().getCurrentLocation().getXPos(),
                    node.getMobilityModel().getCurrentLocation().getYPos(),
                    node.getNumberOfCPUCores(),
                    node.getMipsPerCore(),
                    node.getAvailableRam(),
                    node.getAvailableStorage()
            );
        }
        
        upfTelcoDatacentersInfoPrinted = true;
    }
    
    /**
     * Logs the current position of the UpfTelco sensor with ID
     */
    public void logUpfTelcoPosition(int upfTelcoId, double time, Location position) {
        // Store position in the map
        upfTelcoLocations.put(upfTelcoId, new Location(position.getXPos(), position.getYPos()));
        
        // Add to position history
        positionHistoryMap.computeIfAbsent(upfTelcoId, k -> new ArrayList<>())
                         .add(new Location(position.getXPos(), position.getYPos()));
    }
    
    /**
     * Logs the completion of an UpfTelco task with execution times
     */
    public void logTaskCompletion(Task task) {
        // **CRITICAL: Skip State Estimation tasks - they are handled by logStateEstimationTaskCompletion()**
        String taskType = task.getType();
        if (taskType != null && taskType.contains("GridAnalysisTask")) {
            // State estimation tasks are handled separately
            return;
        }
        
        // Only log PMU data tasks here
        double currentTime = simulationManager.getSimulation().clock();
        
        // Skip tasks without edge device (not PMU data tasks)
        if (task.getEdgeDevice() == null) {
            return;
        }
        
        // Calculate task completion times
        double waitingTime = task.getWatingTime();
        double executionTime = task.getActualCpuTime();
        double networkTime = task.getActualNetworkTime();
        double totalTime = task.getTotalDelay();
        
        // Get task location
        ComputingNode upfTelcoDevice = task.getEdgeDevice();
        Location upfTelcoLocation = upfTelcoDevice != null ? upfTelcoDevice.getMobilityModel().getCurrentLocation() : new Location(0, 0);
        
        // Extract UpfTelco ID 
        int upfTelcoId = extractUpfTelcoIdFromTask(task);
        
        // Get execution location info
        String executionLocation = getExecutionLocation(task);
        
        // Build path information
        String path = getTaskPath((int)task.getId());
        if (path == null || path.isEmpty()) {
            path = buildRealisticPath((int)task.getId(), upfTelcoId);
        }
        
        // Get data sizes
        double dataSizeKB = task.getFileSizeInBits() / 8192.0;
        double returnSizeKB = task.getOutputSizeInBits() / 8192.0;
        
        // Status
        String status = task.getStatus() == Task.Status.SUCCESS ? "S" : "F";
        
        // Create CSV record for PMU data transfer
        String csvRecord = String.format("%.4f,%d,\"(%.1f,%.1f)\",%.2f,\"%s\",%.4f,%s",
            task.getTime(), // Use generation time instead of current time
            upfTelcoId,
            upfTelcoLocation.getXPos(),
            upfTelcoLocation.getYPos(),
            dataSizeKB,
            path,
            totalTime,
            status
        );
        csvRecords.add(csvRecord);
        
        // Create the PMU data transfer log entry
        String taskMessage = String.format(
            "%8.4f | PMU_%d | Location (%7.1f,%7.1f) || Data ID: %6d | Data Size: %6.2f KB || " +
            "Source: PMU | Orchestrator: GNB | Destination: %s || Path: %s || " +
            "Wait: %7.4fs | Exec: %7.4fs | Net: %7.4fs | Total: %7.4fs || Status: [%s] |",
            task.getTime(), // Use generation time for consistency
            upfTelcoId,
            upfTelcoLocation.getXPos(),
            upfTelcoLocation.getYPos(),
            task.getId(),
            dataSizeKB,
            executionLocation,
            path,
            waitingTime,
            executionTime,
            networkTime,
            totalTime,
            status
        );
        
        // Use the custom time printing method
        printWithCustomTime(taskMessage);
        
        // Write to log file
        if (logWriter != null) {
            logWriter.println(taskMessage);
            logWriter.flush();
        }
    }
    
    /**
     * Extract UpfTelco ID from task
     */
    private int extractUpfTelcoIdFromTask(Task task) {
        try {
            // Try to extract from edge device first
            if (task.getEdgeDevice() != null) {
                String deviceName = task.getEdgeDevice().getName();
                if (deviceName != null && deviceName.contains("_")) {
                    return Integer.parseInt(deviceName.split("_")[1]);
                }
            }
            
            // Fallback: use task ID modulo
            return (int)(task.getId() % 20); // Assuming max 20 UpfTelco devices
            
        } catch (Exception e) {
            return 0; // Default fallback
        }
    }
    
    /**
     * Print with custom time formatting
     */
    private void printWithCustomTime(String message) {
        if (simulationManager == null) return;
        
        double time = simulationManager.getSimulation().clock();
        String formattedTime = String.format("%.4f", time);
        
        // Add date and time with correct formatting
        String fullMessage = String.format("%s - simulation time %s (s) : %s",
            dateFormat.format(new Date()),
            formattedTime,
            message);
        
        // Check if should print to terminal
        if (PRINT_TO_TERMINAL) {
            simulationManager.getSimulationLogger().printWithoutTime(fullMessage);
        } else {
            // Write only to log file without printing to terminal
            try {
                String logFileName = simulationManager.getSimulationLogger().getFileName(".txt");
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(logFileName, true))) {
                    writer.write(fullMessage);
                    writer.newLine();
                }
            } catch (IOException e) {
                // If writing to file fails, use normal method
                simulationManager.getSimulationLogger().printWithoutTime(fullMessage);
            }
        }
    }
    
    /**
     * Simple log method
     */
    public void log(String message) {
        printWithCustomTime(message);
        
        if (logWriter != null) {
            logWriter.println(message);
            logWriter.flush();
        }
    }
    
    /**
     * Log method with formatting
     */
    public void log(String format, Object... args) {
        String message = String.format(format, args);
        log(message);
    }
    
    /**
     * Save CSV logs to file
     */
    public void saveCSVLog() {
        String csvFileName = getCSVFileName();
        try (PrintWriter csvPrintWriter = new PrintWriter(new FileWriter(csvFileName))) {
            csvPrintWriter.println(CSV_HEADER);
            for (String record : csvRecords) {
                csvPrintWriter.println(record);
            }
        } catch (IOException e) {
            System.err.println("UpfTelcoLogger - Failed to save CSV log: " + e.getMessage());
        }
    }
    
    /**
     * Get execution location description
     */
    private String getExecutionLocation(Task task) {
        ComputingNode executionNode = task.getOrchestrator();
        if (executionNode != null) {
            return executionNode.getName();
        }
        return "Unknown";
    }
    
    /**
     * Get CSV file name
     */
    private String getCSVFileName() {
        String baseFileName = simulationManager.getSimulationLogger().getFileName("");
        return baseFileName + "_pmu_data_transfers.csv";
    }
    
    /**
     * Save all logs and close
     */
    public void saveAllLogs() {
        try {
            // Save PMU data transfer CSV
            saveCSVLog();
            
            // **NEW: Save State Estimation CSV**
            saveStateEstimationCSV();
            
            // **NEW: Save Network Usage CSV**
            saveNetworkUsageCSV();
            
        } catch (Exception e) {
            System.err.println("UpfTelcoLogger - Error saving logs: " + e.getMessage());
        }
    }
    
    /**
     * **NEW: Save State Estimation CSV**
     */
    private void saveStateEstimationCSV() {
        String csvFileName = getStateEstimationCSVFileName();
        try (PrintWriter csvPrintWriter = new PrintWriter(new FileWriter(csvFileName))) {
            csvPrintWriter.println(STATE_ESTIMATION_CSV_HEADER);
            for (String record : stateEstimationCsvRecords) {
                csvPrintWriter.println(record);
            }
        } catch (IOException e) {
            System.err.println("UpfTelcoLogger - Failed to save State Estimation CSV: " + e.getMessage());
        }
    }
    
    /**
     * Get hop key for tracking
     */
    private String getHopKey(int taskId, ComputingNode from, ComputingNode to) {
        return taskId + "_" + from.getName() + "_" + to.getName();
    }
    
    public void updateHopTime(Task task, ComputingNode from, ComputingNode to, double time) {
        String key = getHopKey((int)task.getId(), from, to);
        hopTimes.put(key, time);
    }
    
    public void logHop(Task task, ComputingNode from, ComputingNode to, double transferTime) {
        String key = getHopKey((int)task.getId(), from, to);
        hopTimes.put(key, transferTime);
        
        String message = String.format("Task %d: %s -> %s (%.4fs)", 
                                     task.getId(), from.getName(), to.getName(), transferTime);
        log(message);
    }
    
    /**
     * Close the logger
     */
    public void close() {
        if (logWriter != null) {
            logWriter.close();
        }
        if (csvWriter != null) {
            csvWriter.close();
        }
    }
    
    /**
     * Store expected path for a task
     */
    public void storeExpectedPath(int taskId, String expectedPath) {
        if (expectedPath != null && !expectedPath.trim().isEmpty()) {
            expectedPaths.put(taskId, expectedPath);
        }
    }
    
    /**
     * Store GNB name for task
     */
    public void storeTaskGnb(int taskId, String gnbName) {
        if (gnbName != null) {
            taskGnbNames.put(taskId, gnbName);
        }
    }
    
    /**
     * Get path for task
     */
    private String getTaskPath(int taskId) {
        // First try stored expected path
        String expectedPath = expectedPaths.get(taskId);
        if (expectedPath != null && !expectedPath.trim().isEmpty()) {
            return expectedPath;
        }
        
        // Try to find the task and build path
        Task task = findTaskById(taskId);
        if (task != null) {
            return getTaskPathWithDelays(task);
        }
        
        return null;
    }
    
    /**
     * Build path with delays from hop times
     */
    private String buildPathWithDelays(Task task, Map<Integer, Double> hopDelays) {
        StringBuilder pathBuilder = new StringBuilder();
        
        // Get source device
        ComputingNode sourceDevice = task.getEdgeDevice();
        if (sourceDevice != null) {
            pathBuilder.append("PMU_").append(extractUpfTelcoIdFromTask(task));
        } else {
            pathBuilder.append("PMU_?");
        }
        
        // Get GNB name from stored task GNB names
        String gnbName = taskGnbNames.get((int)task.getId());
        if (gnbName != null) {
            // Look for hop delay to this GNB
            String hopKey = (int)task.getId() + "_" + sourceDevice.getName() + "_" + gnbName;
            Double hopDelay = hopTimes.get(hopKey);
            
            if (hopDelay != null) {
                pathBuilder.append(" -> ").append(gnbName).append(" (").append(String.format("%.4f", hopDelay)).append("s)");
            } else {
                pathBuilder.append(" -> ").append(gnbName).append(" (?.??s)");
            }
        } else {
            pathBuilder.append(" -> GNB_? (?.??s)");
        }
        
        return pathBuilder.toString();
    }
    
    /**
     * Find task by ID
     */
    private Task findTaskById(int taskId) {
        // This is a simplified implementation
        // In a real scenario, you might need to maintain a task registry
        return null;
    }
    
    /**
     * Get task path with delays
     */
    private String getTaskPathWithDelays(Task task) {
        return buildPathWithDelays(task, new HashMap<>());
    }
    
    /**
     * Extract batch information from task type
     */
    private String extractBatchInfo(Task task) {
        String taskType = task.getType();
        if (taskType != null && taskType.contains(":")) {
            String[] parts = taskType.split(":");
            if (parts.length >= 2) {
                return parts[1]; // Return batch information
            }
        }
        return "";
    }
    
    /**
     * Flush all writers
     */
    public void flush() {
        if (logWriter != null) {
            logWriter.flush();
        }
        if (csvWriter != null) {
            csvWriter.flush();
        }
    }
    
    /**
     * Log UpfTelco data transfer
     */
    public void logUpfTelcoDataTransfer(int upfTelcoId, double dataSizeKB, double currentTime, double timeWindow) {
        String message = String.format("UpfTelco_%d transferred %.2f KB at time %.3f (window: %.1f)", 
                                     upfTelcoId, dataSizeKB, currentTime, timeWindow);
        log(message);
    }
    
    /**
     * Log UpfTelco data transfer with full details
     */
    public void logUpfTelcoDataTransferFull(Task dataTask, int upfTelcoId, double dataSizeKB, String path) {
        logUpfTelcoDataTransferFull(dataTask, upfTelcoId, dataSizeKB, path, false);
    }
    
    public void logUpfTelcoDataTransferFull(Task dataTask, int upfTelcoId, double dataSizeKB, String path, boolean deadlineMissed) {
        logUpfTelcoDataTransferFull(dataTask, upfTelcoId, dataSizeKB, path, deadlineMissed, 0.0);
    }
    
    public void logUpfTelcoDataTransferFull(Task dataTask, int upfTelcoId, double dataSizeKB, String path, boolean deadlineMissed, double hopSum) {
        // **NEW: Use generation time instead of simulation clock**
        double generationTime = dataTask.getTime();
        
        // Get PMU location
        ComputingNode pmu = dataTask.getEdgeDevice();
        Location pmuLocation = pmu != null ? pmu.getMobilityModel().getCurrentLocation() : new Location(0, 0);
        
        // **FIXED: Move deadline flag after hop sum**
        String deadlineFlag = deadlineMissed ? " [DEADLINE_MISSED]" : "";
        String hopSumInfo = hopSum > 0.0 ? String.format(" | Hop_Sum: %.4fs", hopSum) : "";
        
        String logMessage = String.format(
            "%8.4f | PMU_%d | Location (%7.1f,%7.1f) || Data ID: %6d | Data Size: %6.2f KB || " +
            "Source: PMU | Orchestrator: GNB | Path: %s |%s%s",
            generationTime,
            upfTelcoId,
            pmuLocation.getXPos(),
            pmuLocation.getYPos(),
            dataTask.getId(),
            dataSizeKB,
            path,
            hopSumInfo,
            deadlineFlag
        );
        
        // Use the custom time printing method
        printWithCustomTime(logMessage);
        
        // Write to log file
        if (logWriter != null) {
            logWriter.println(logMessage);
            logWriter.flush();
        }
        
        // **NEW: Also add to CSV records**
        String status = deadlineMissed ? "L" : "S"; // L = Late, S = Success
        String csvRecord = String.format("%.4f,%d,\"(%.1f,%.1f)\",%.2f,\"%s\",%.4f,%s",
            generationTime,
            upfTelcoId,
            pmuLocation.getXPos(),
            pmuLocation.getYPos(),
            dataSizeKB,
            path,
            hopSum,
            status
        );
        csvRecords.add(csvRecord);
    }
    
    /**
     * **NEW: Log state estimation task completion with comprehensive statistics**
     */
    public void logStateEstimationTaskCompletion(Task stateTask, boolean success) {
        double currentTime = simulationManager.getSimulation().clock();
        
        // Calculate detailed timing information
        double waitingTime = stateTask.getWatingTime();
        double executionTime = stateTask.getActualCpuTime();
        double networkTime = stateTask.getActualNetworkTime();
        double totalTime = stateTask.getTotalDelay();
        
        // Get execution location (should be a GNB)
        ComputingNode gnb = stateTask.getOffloadingDestination();
        String gnbName = gnb != null ? gnb.getName() : "Unknown_GNB";
        Location gnbLocation = gnb != null ? gnb.getMobilityModel().getCurrentLocation() : new Location(0, 0);
        
        // Extract GNB ID from name (e.g., "EDGE_1" -> "1")
        String gnbId = "Unknown";
        if (gnbName.startsWith("EDGE_")) {
            try {
                gnbId = gnbName.substring(5);
            } catch (Exception e) {
                gnbId = "Unknown";
            }
        }
        
        // Get data sizes
        double dataSizeKB = stateTask.getFileSizeInBits() / 8192.0;
        double returnSizeKB = stateTask.getOutputSizeInBits() / 8192.0;
        
        // Parse batch information from task type
        String windowInfo = "?.?";
        String coverageInfo = "[?/?]";
        String batchType = "?";
        
        String taskType = stateTask.getType();
        if (taskType != null && taskType.contains(":")) {
            String[] parts = taskType.split(":");
            if (parts.length >= 4) {
                windowInfo = parts[1];
                coverageInfo = "[" + parts[2] + "]";
                batchType = parts[3];
            }
        }
        
        // Status
        boolean taskSuccess = (stateTask.getStatus() == Task.Status.SUCCESS);
        String status = taskSuccess ? "S" : "F";
        
        // **NEW: Get PDC waiting time for this task**
        double pdcWaitingTime = taskCollectionTimes.getOrDefault(stateTask.getId(), 0.0);
        
        // **NEW: Get first data network delay for this task (from Hop_Sum of first arriving PMU data)**
        double firstDataNetworkDelay = taskFirstDataNetworkDelays.getOrDefault(stateTask.getId(), 0.0);
        
        // **NEW: Calculate custom total time = First Data Network Delay + PDC Waiting Time + Execution Time**
        double customTotalTime = firstDataNetworkDelay + pdcWaitingTime + executionTime;
        
        // DEBUG: Print timing calculation
        System.out.println("DEBUG - Task " + stateTask.getId() + ": FirstDataNetDelay=" + firstDataNetworkDelay + 
                          " + PDCWait=" + pdcWaitingTime + " + ExecTime=" + executionTime + 
                          " = CustomTotal=" + customTotalTime);
        
        // **NEW: Add to State Estimation CSV with GNB ID using custom total time**
        String stateEstimationCsvRecord = String.format("%.4f,%d,%s,%s,%s,%s,%.2f,%.2f,%.2f,%.0f,%.4f,%.4f,%.4f,%.4f,%s,%.4f,%d",
            currentTime,
            stateTask.getId(),
            gnbId,  // New GNB ID field
            windowInfo,
            coverageInfo.replace("[", "").replace("]", ""), // Remove brackets for CSV
            batchType,
            dataSizeKB,
            returnSizeKB,
            stateTask.getMaxLatency(),
            stateTask.getLength(),
            waitingTime,
            executionTime,
            networkTime,
            customTotalTime,  // Use custom calculated total time instead of totalTime
            status,
            pdcWaitingTime,
            taskSuccess ? 1 : 0  // Success flag as integer for easier analysis
        );
        stateEstimationCsvRecords.add(stateEstimationCsvRecord);
        
        // Create the State Estimation task completion log entry (GNB-based)
        String taskMessage = String.format(
            "%8.4f | GNB_%s: State Estimation Task %s | Location (%7.1f,%7.1f) || Task ID: %6d | Data Size: %6.2f KB | Return: %6.2f KB | MaxLatency: %5.2f s | Length: %5.0f MI || " +
            "Source: GNB_%s | Orchestrator: GNB_%s | Destination: GNB_%s || Wait: %7.4fs | Exec: %7.4fs | Net: %7.4fs | Total: %7.4fs || Status: [%s] || Window: %s | PDC Waiting: %6.4fs |",
            currentTime,
            gnbId,  // GNB_X format
            coverageInfo,
            gnbLocation.getXPos(),
            gnbLocation.getYPos(),
            stateTask.getId(),
            dataSizeKB,
            returnSizeKB,
            stateTask.getMaxLatency(),
            stateTask.getLength(),
            gnbId,  // Source: GNB_X
            gnbId,  // Orchestrator: GNB_X  
            gnbId,  // Destination: GNB_X (local execution)
            waitingTime,
            executionTime,
            networkTime,
            customTotalTime,  // Use custom calculated total time
            status,
            windowInfo,
            pdcWaitingTime
        );
        
        // Use the custom time printing method
        printWithCustomTime(taskMessage);
        
        // Write to log file
        if (logWriter != null) {
            logWriter.println(taskMessage);
            logWriter.flush();
        }
        
        // **NEW: Clean up PDC waiting time data**
        taskCollectionTimes.remove(stateTask.getId());
    }
    
    /**
     * Logs state estimation task creation and execution (detailed) - SIMPLIFIED
     */
    public void logStateEstimationTask(int taskId, double timeWindow, int upfTelcoCount, 
                                     int totalUpfTelcos, String batchType, double totalDataKB) {
        // Simple notification that state estimation task was created
        String message = String.format("State Estimation Task %d created: Window %.1f, Coverage %d/%d (%s), Data %.2f KB", 
                                     taskId, timeWindow, upfTelcoCount, totalUpfTelcos, batchType, totalDataKB);
        log(message);
        flush();
    }
    
    /**
     * NEW: Logs state estimation task creation with PDC waiting timing
     */
    public void logStateEstimationTaskWithTiming(int taskId, double timeWindow, int upfTelcoCount, 
                                               int totalUpfTelcos, String batchType, double totalDataKB, double pdcWaitingTime) {
        // Enhanced notification with PDC waiting time
        String message = String.format("State Estimation Task %d created: Window %.1f, Coverage %d/%d (%s), Data %.2f KB, PDC Waiting Time: %.4fs", 
                                     taskId, timeWindow, upfTelcoCount, totalUpfTelcos, batchType, totalDataKB, pdcWaitingTime);
        log(message);
        flush();
        
        // Store PDC waiting time for this task
        taskCollectionTimes.put(taskId, pdcWaitingTime);
    }
    
    /**
     * NEW: Store first data network delay for a task
     */
    public void storeFirstDataNetworkDelay(int taskId, double firstDataNetworkDelay) {
        taskFirstDataNetworkDelays.put(taskId, firstDataNetworkDelay);
    }
    
    /**
     * **NEW: Track network data volume for each network layer**
     */
    public void trackNetworkVolume(String networkLevel, double dataSizeKB) {
        networkVolumePerLayer.put(networkLevel, 
            networkVolumePerLayer.getOrDefault(networkLevel, 0.0) + dataSizeKB);
        transferCountPerLayer.put(networkLevel, 
            transferCountPerLayer.getOrDefault(networkLevel, 0) + 1);
    }
    
    /**
     * **NEW: Get total network volume for a specific layer**
     */
    public double getNetworkVolume(String networkLevel) {
        return networkVolumePerLayer.getOrDefault(networkLevel, 0.0);
    }
    
    /**
     * **NEW: Get transfer count for a specific layer**
     */
    public int getTransferCount(String networkLevel) {
        return transferCountPerLayer.getOrDefault(networkLevel, 0);
    }
    
    /**
     * **FIXED: Logs when a GNB data collection window completes - showing PMUs assigned to this GNB**
     */
    public void logDataCollectionComplete(double generationTime, int pmuCount, int totalPmuForThisGnb, boolean isComplete) {
        String batchType = isComplete ? "COMPLETE" : "TIMEOUT";
        String message = String.format(">>> GNB Data Collection for Generation Time %.1f: %d/%d PMU devices [%s] <<<", 
                                     generationTime, pmuCount, totalPmuForThisGnb, batchType);
        log(message);
        flush();
    }
    
    /**
     * **DEPRECATED: Old method - use the version with totalPmuForThisGnb instead**
     */
    public void logDataCollectionComplete(double generationTime, int upfTelcoCount, boolean isComplete) {
        String batchType = isComplete ? "COMPLETE" : "TIMEOUT";
        String message = String.format(">>> GNB Data Collection for Generation Time %.1f: %d/%d PMU devices [%s] <<<", 
                                     generationTime, upfTelcoCount, totalUpfTelcoCount, batchType);
        log(message);
        flush();
    }
    
    /**
     * Stores hop times for a task from the network model (3-hop path)
     */
    public void storeTaskHopTimes(int taskId, String gnbName, double pmuToGnbTime, double gnbToTelcoTime, double telcoToGnbTime) {
        taskHopTimes.put(taskId, new TaskHopInfo(gnbName, pmuToGnbTime, gnbToTelcoTime, telcoToGnbTime));
    }
    
    /**
     * Gets stored hop times for a task
     */
    public TaskHopInfo getTaskHopTimes(int taskId) {
        return taskHopTimes.get(taskId);
    }
    
    /**
     * Builds realistic path string with actual hop times (3-hop path: PMU → GNB → TELCO → GNB)
     */
    public String buildRealisticPath(int taskId, int pmuId) {
        TaskHopInfo hopInfo = taskHopTimes.get(taskId);
        
        if (hopInfo != null) {
            // Build path with actual hop times for 3-hop path
            return String.format("PMU_%d -> %s (%.4fs) -> TELCO (%.4fs) -> %s (%.4fs)", 
                               pmuId, hopInfo.gnbName, hopInfo.pmuToGnbTime,
                               hopInfo.gnbToTelcoTime, hopInfo.gnbName, hopInfo.telcoToGnbTime);
        } else {
            // Fallback to template for 3-hop path
            return String.format("PMU_%d -> GNB_? (?.??s) -> TELCO (?.??s) -> GNB_? (?.??s)", pmuId);
        }
    }
    
    // **NEW: Helper method for State Estimation CSV file name**
    private String getStateEstimationCSVFileName() {
        String baseFileName = simulationManager.getSimulationLogger().getFileName("");
        // Add "state_estimation_" suffix before the .csv extension
        return baseFileName + "_state_estimation.csv";
    }
    
    /**
     * **NEW: Logs grid analysis task creation for distributed GNB collection**
     */
    public void logStateEstimationTaskCreation(Task analysisTask, int collectionId, int upfTelcoCount, 
                                             int totalUpfTelcos, double pdcWaitingTime, double totalDataKB, String batchType) {
        // Enhanced notification with collection ID and PDC waiting time
        ComputingNode gnb = analysisTask.getOffloadingDestination();
        String gnbName = gnb != null ? gnb.getName() : "Unknown_GNB";
        
        String message = String.format("Grid Analysis Task %d created on %s: Collection %d, Coverage %d/%d (%s), Data %.2f KB, PDC Waiting Time: %.4fs", 
                                     analysisTask.getId(), gnbName, collectionId, upfTelcoCount, totalUpfTelcos, batchType, totalDataKB, pdcWaitingTime);
        log(message);
        flush();
        
        // Store PDC waiting time for this task
        taskCollectionTimes.put((int)analysisTask.getId(), pdcWaitingTime);
    }
    
    /**
     * Logs grid analysis task completion with detailed statistics
     */
    public void logGridAnalysisTaskCompletion(Task analysisTask, boolean success) {
        logStateEstimationTaskCompletion(analysisTask, success);
    }
    
    /**
     * Log UpfTelco network transfer using NetworkTransferResult from UpfTelcoNetworkModel
     */
    public void logUpfTelcoNetworkTransfer(com.mechalikh.pureedgesim.taskgenerator.Task dataTask, 
                                    UpfOnTelco_PdcOnEdge.UpfTelcoNetworkModel.NetworkTransferResult transferResult) {
        try {
            // Extract UpfTelco ID from task
            int upfTelcoId = extractUpfTelcoIdFromTask(dataTask);
            
            // Convert data size to KB
            double dataSizeKB = 2.0; // Fixed 2KB UpfTelco data
            
            // Use the path string from transfer result
            String path = transferResult.getPathString();
            
            // **NEW: Track network volume for each hop**
            trackNetworkVolume("PMU_to_GNB", dataSizeKB);
            trackNetworkVolume("GNB_to_TELCO", dataSizeKB);
            trackNetworkVolume("TELCO_to_GNB", dataSizeKB);
            
            // Log the transfer with all details
            logUpfTelcoDataTransferFull(dataTask, upfTelcoId, dataSizeKB, path, false, transferResult.totalDelay);
            
        } catch (Exception e) {
            System.err.println("UpfTelcoLogger - Error logging network transfer: " + e.getMessage());
        }
    }
    
    /**
     * **NEW: Save network usage statistics to CSV**
     */
    public void saveNetworkUsageCSV() {
        try {
            String csvFileName = getNetworkUsageCSVFileName();
            
            // Create network usage statistics
            for (String networkLevel : networkVolumePerLayer.keySet()) {
                double totalVolume = networkVolumePerLayer.get(networkLevel);
                int transferCount = transferCountPerLayer.get(networkLevel);
                double avgDataSize = transferCount > 0 ? totalVolume / transferCount : 0.0;
                
                String csvRecord = String.format("%s,%.6f,%d,%.6f",
                    networkLevel, totalVolume, transferCount, avgDataSize);
                networkUsageCsvRecords.add(csvRecord);
            }
            
            // Write to CSV file
            try (PrintWriter writer = new PrintWriter(new FileWriter(csvFileName))) {
                writer.println(NETWORK_USAGE_CSV_HEADER);
                for (String record : networkUsageCsvRecords) {
                    writer.println(record);
                }
            }
            
            System.out.println("UpfTelcoLogger - Network usage CSV saved: " + csvFileName);
            
        } catch (IOException e) {
            System.err.println("UpfTelcoLogger - Error saving network usage CSV: " + e.getMessage());
        }
    }
    
    /**
     * **NEW: Helper method for Network Usage CSV file name**
     */
    private String getNetworkUsageCSVFileName() {
        String baseFileName = simulationManager.getSimulationLogger().getFileName("");
        return baseFileName + "_network_usage.csv";
    }
} 