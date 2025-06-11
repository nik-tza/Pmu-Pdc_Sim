package UpfOnTelco_PdcOnCloud;

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
 * PMU Logger for Smart Grid simulation logging.
 * Uses the Singleton pattern to ensure a single instance throughout the system.
 * Logs detailed information about PMU sensor tasks similar to DroneLogger.
 */
public class CloudLogger {
    private static CloudLogger instance = null;
    private PrintWriter logWriter;
    private PrintWriter csvWriter;
    private SimpleDateFormat dateFormat;
    private String outputPath;
    private SimulationManager simulationManager;
    
    // Control printing to terminal
    private static boolean PRINT_TO_TERMINAL = false; 
    
    // Maps for storing information per PMU sensor
    private Map<Integer, Location> pmuLocations = new HashMap<>();
    
    // Map for position history per PMU sensor
    private Map<Integer, List<Location>> positionHistoryMap = new HashMap<>();
    
    // Store task information per second
    private boolean edgeDatacentersInfoPrinted = false;
    
    // **NEW: Dynamic PMU count**
    private int totalPmuCount = 14; // Default fallback
    
    // Adding new fields for CSV logging
    private List<String> csvRecords = new ArrayList<>();
    private static final String CSV_HEADER = "Time,PmuID,PmuCoordinates,DataSize,Path,HopSum,Status";
    
    // **NEW: State Estimation CSV logging**
    private List<String> stateEstimationCsvRecords = new ArrayList<>();
    private static final String STATE_ESTIMATION_CSV_HEADER = "Time,TaskID,Window,Coverage,BatchType,InputDataKB,OutputDataKB,MaxLatency,ComputationMI,WaitTime,ExecTime,NetTime,TotalTime,Status,PDCWaitingTime,SuccessFlag";
    
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
    
    /**
     * Stores hop times for a task
     */
    public static class TaskHopInfo {
        public final String gnbName;
        public final double pmuToGnbTime;
        public final double gnbToTelcoTime;
        public final double telcoToTsoTime;
        
        public TaskHopInfo(String gnbName, double pmuToGnbTime, double gnbToTelcoTime, double telcoToTsoTime) {
            this.gnbName = gnbName;
            this.pmuToGnbTime = pmuToGnbTime;
            this.gnbToTelcoTime = gnbToTelcoTime;
            this.telcoToTsoTime = telcoToTsoTime;
        }
    }
    
    private CloudLogger(String path) throws IOException {
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
    
    public static CloudLogger initialize(SimulationManager simulationManager, String outputPath) throws IOException {
        if (instance == null) {
            instance = new CloudLogger(outputPath);
        }
        instance.simulationManager = simulationManager;
        
        // **NEW: Read dynamic PMU count from simulation parameters**
        instance.totalPmuCount = SimulationParameters.maxNumberOfEdgeDevices;
        
        return instance;
    }
    
    public static CloudLogger initialize(String outputPath) throws IOException {
        if (instance == null) {
            instance = new CloudLogger(outputPath);
        }
        return instance;
    }
    
    public static CloudLogger getInstance() {
        if (instance == null) {
            throw new IllegalStateException("CloudLogger has not been initialized. Call initialize() first.");
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
     * Prints information about edge datacenters.
     */
    public void printEdgeDatacentersInfo() {
        if (edgeDatacentersInfoPrinted || simulationManager == null || 
            simulationManager.getDataCentersManager() == null) {
            return;
        }
        
        List<ComputingNode> edgeDatacenters = simulationManager.getDataCentersManager()
                .getComputingNodesGenerator().getEdgeOnlyList();
        
        simulationManager.getSimulationLogger().printWithoutTime("===== EDGE DATACENTERS =====");
        for (ComputingNode node : edgeDatacenters) {
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
        
        edgeDatacentersInfoPrinted = true;
    }
    
    /**
     * Logs the current position of the PMU sensor with ID
     */
    public void logPmuPosition(int pmuId, double time, Location position) {
        // Store position in the map
        pmuLocations.put(pmuId, new Location(position.getXPos(), position.getYPos()));
        
        // Add to position history
        positionHistoryMap.computeIfAbsent(pmuId, k -> new ArrayList<>())
                         .add(new Location(position.getXPos(), position.getYPos()));
    }
    
    /**
     * Logs the completion of a PMU task with execution times
     */
    public void logTaskCompletion(Task task) {
        // **CRITICAL: Skip State Estimation tasks - they are handled by logStateEstimationTaskCompletion()**
        String taskType = task.getType();
        if (taskType != null && taskType.startsWith("STATE_ESTIMATION:")) {
            // State Estimation tasks are handled by logStateEstimationTaskCompletion()
            return;
        }
        
        // **ADDITIONAL CHECK: Skip TSO-sourced tasks (they are State Estimation tasks)**
        ComputingNode sourceDevice = task.getEdgeDevice();
        if (sourceDevice != null && sourceDevice.getName() != null && 
            sourceDevice.getName().equals("TSO")) {
            // This is a TSO task, likely State Estimation - skip it
            return;
        }
        
        // **ADDITIONAL CHECK: Skip tasks with TSO location (-1.0, -1.0)**
        if (sourceDevice != null) {
            Location loc = sourceDevice.getMobilityModel().getCurrentLocation();
            if (loc.getXPos() == -1.0 && loc.getYPos() == -1.0) {
                // This task comes from TSO/Cloud location - skip it
                return;
            }
        }
        
        String timestamp = dateFormat.format(new Date());
        ComputingNode pmu = task.getEdgeDevice();
        String status = task.getStatus() == Task.Status.SUCCESS ? "S" : "F";
        
        // Get the actual network path (hops) with delays or expected path
        String hops = getTaskPathWithDelays(task);
            
        // Get additional task information
        double waitingTime = task.getWatingTime();
        double executionTime = task.getActualCpuTime();
        double networkTime = task.getActualNetworkTime();
        double totalTime = task.getTotalDelay();
        
        // Find PMU ID
        int pmuId = -1;
        List<ComputingNode> pmuDevices = simulationManager.getDataCentersManager()
                .getComputingNodesGenerator().getMistOnlyList();
        
        for (int i = 0; i < pmuDevices.size(); i++) {
            if (pmuDevices.get(i).equals(task.getEdgeDevice())) {
                pmuId = i; // PMU IDs start from 0 (0-13 for 14 devices)
                break;
            }
        }
        
        // Get PMU location
        Location pmuLocation = pmu.getMobilityModel().getCurrentLocation();
        
        // Get destination info
        ComputingNode destination = task.getOffloadingDestination();
        String destinationName = destination != null && destination.getName() != null && !destination.getName().trim().isEmpty() 
                                ? destination.getName() : "Unknown";
        
        // Calculate data sizes
        double dataSizeKB = task.getFileSizeInBits() / 8192.0; // Convert bits to KB
        double returnSizeKB = task.getOutputSizeInBits() / 8192.0; // Convert bits to KB
        
        // Extract batch information from task description, or create default based on PMU ID
        String batchInfo = extractBatchInfo(task);
        if (batchInfo.equals("[?/" + totalPmuCount + "]")) {
            // If no batch info found, create one based on PMU ID
            batchInfo = String.format("[%d/%d]", pmuId + 1, totalPmuCount); // PMU IDs 1-totalPmuCount
        }
        
        // Get current simulation time
        double simTime = simulationManager != null ? simulationManager.getSimulation().clock() : 0.0;
        
        // Create detailed message with simulation time at the beginning and batch info prominently displayed
        String taskMessage = String.format(
            "%.4f | PMU: %d %s| Location (%.1f,%.1f) || Task ID: %6d |  Data Size: %.2f KB | Return: %.2f KB | MaxLatency: %.2f s | Length: %.0f MI || " +
            "Source: PMU | Orchestrator: PMU | Destination: %s || Wait: %.4fs | Exec: %.4fs | Net: %.4fs | Total: %.4fs || Status: [%s] || Path: %s |",
            simTime,
            pmuId,
            batchInfo.isEmpty() ? "" : batchInfo + " ",  // Add batch info right after PMU ID
            pmuLocation.getXPos(),
            pmuLocation.getYPos(),
            task.getId(),
            dataSizeKB,
            returnSizeKB,
            task.getMaxLatency(),
            task.getLength(),
            destinationName,
            waitingTime,
            executionTime,
            networkTime,
            totalTime,
            status,
            hops
        );
        
        // Use the custom time printing method like DroneLogger
        printWithCustomTime(taskMessage);
        
        // Also write to CSV
        String csvRecord = String.format("%s,%d,%d,%.2f,%.2f,%s",
            timestamp, pmuId, pmuId + 1, dataSizeKB, returnSizeKB,
            status, waitingTime, executionTime, networkTime, totalTime, hops);
        
        csvRecords.add(csvRecord);
        
        // Write to log file
        if (logWriter != null) {
            logWriter.println(taskMessage);
            logWriter.flush();
        }
        
        // **NEW: Clean up PDC waiting time data**
        taskCollectionTimes.remove(task.getId());
    }
    
    // Add new method for printing with specific time formatting (like DroneLogger)
    private void printWithCustomTime(String message) {
        if (simulationManager == null) return;
        
        double time = simulationManager.getSimulation().clock();
        String formattedTime = String.format("%.4f", time);
        
        // Add date and time with correct formatting
        String fullMessage = String.format("%s - simulation time %s (s) : %s",
            new java.text.SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new java.util.Date()),
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
     * Logs a message with timestamp
     */
    public void log(String message) {
        printWithCustomTime(message);
        
        // Also write to log file
        if (logWriter != null) {
            logWriter.println(dateFormat.format(new Date()) + " - " + message);
            logWriter.flush();
        }
    }
    
    /**
     * Logs a formatted message with timestamp
     */
    public void log(String format, Object... args) {
        String message = String.format(format, args);
        printWithCustomTime(message);
        
        // Also write to log file
        if (logWriter != null) {
            logWriter.println(dateFormat.format(new Date()) + " - " + message);
            logWriter.flush();
        }
    }
    
    // Method to save the CSV file
    public void saveCSVLog() {
        String csvFileName = getCSVFileName();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvFileName))) {
            writer.write(CSV_HEADER);
            writer.newLine();
            for (String record : csvRecords) {
                writer.write(record);
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    // Helper method to determine execution location
    private String getExecutionLocation(Task task) {
        ComputingNode destination = task.getOffloadingDestination();
        if (destination.getType() == SimulationParameters.TYPES.EDGE_DEVICE) {
            return "Local PMU";
        } else if (destination.getType() == SimulationParameters.TYPES.EDGE_DATACENTER) {
            String name = destination.getName();
            return "Edge Server: " + (name != null && !name.trim().isEmpty() ? name : "Unknown");
        } else {
            String name = destination.getName();
            return "Cloud: " + (name != null && !name.trim().isEmpty() ? name : "TSO");
        }
    }
    
    // Helper method for CSV file name
    private String getCSVFileName() {
        String baseFileName = simulationManager.getSimulationLogger().getFileName("");
        // Add "pmu_" prefix before the .csv extension
        return baseFileName + "_pmu.csv";
    }
    
    // Add method to save CSV at the end of simulation
    public void saveAllLogs() {
        // Save PMU data CSV (Sequential_simulation_pmu.csv for analysis)
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(getCSVFileName()))) {
            writer.write(CSV_HEADER);
            writer.newLine();
            for (String record : csvRecords) {
                writer.write(record);
                writer.newLine();
            }
            System.out.println("CloudLogger - Saved " + csvRecords.size() + " PMU data transfer records to CSV");
        } catch (IOException e) {
            System.out.println("CloudLogger - Error saving PMU data CSV: " + e.getMessage());
            e.printStackTrace();
        }
        
        // **NEW: Save State Estimation CSV**
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(getStateEstimationCSVFileName()))) {
            writer.write(STATE_ESTIMATION_CSV_HEADER);
            writer.newLine();
            for (String record : stateEstimationCsvRecords) {
                writer.write(record);
                writer.newLine();
            }
            System.out.println("CloudLogger - Saved " + stateEstimationCsvRecords.size() + " State Estimation task records to CSV");
        } catch (IOException e) {
            System.out.println("CloudLogger - Error saving State Estimation CSV: " + e.getMessage());
            e.printStackTrace();
        }
        
        // Clear the lists
        csvRecords.clear();
        stateEstimationCsvRecords.clear();
        
        // Close writers
        if (logWriter != null) {
            logWriter.close();
        }
        // Removed csvWriter
    }
    
    private String getHopKey(int taskId, ComputingNode from, ComputingNode to) {
        return String.format("%d_%s->%s", taskId, from.getName(), to.getName());
    }
    
    public void updateHopTime(Task task, ComputingNode from, ComputingNode to, double time) {
        String hopKey = getHopKey(task.getId(), from, to);
        hopTimes.put(hopKey, time);
    }
    
    public void logHop(Task task, ComputingNode from, ComputingNode to, double transferTime) {
        StringBuilder hops = taskHops.computeIfAbsent(task.getId(), k -> new StringBuilder());
        String hopKey = getHopKey(task.getId(), from, to);
        
        // If we have an actual transfer time for this hop, use it
        double actualTime = hopTimes.getOrDefault(hopKey, transferTime);
        
        String hopInfo = String.format("%s->%s (%.4fs), ", 
            from.getName(), 
            to.getName(),
            actualTime);
            
        hops.append(hopInfo);
    }
    
    public void close() {
        if (logWriter != null) {
            logWriter.close();
        }
        // Removed csvWriter
    }
    
    /**
     * Stores the expected path for a task
     */
    public void storeExpectedPath(int taskId, String expectedPath) {
        expectedPaths.put(taskId, expectedPath);
    }
    
    /**
     * Stores the GNB name for a task
     */
    public void storeTaskGnb(int taskId, String gnbName) {
        taskGnbNames.put(taskId, gnbName);
    }
    
    /**
     * Gets the actual network path for a task, or expected path if actual is not available
     */
    private String getTaskPath(int taskId) {
        // First try to get actual hops with delays from the task
        if (simulationManager != null) {
            // Find the task to get its hop delays
            Task currentTask = findTaskById(taskId);
            if (currentTask != null) {
                Map<Integer, Double> hopDelays = currentTask.getAllHopDelays();
                if (!hopDelays.isEmpty()) {
                    // Build path with actual hop delays
                    return buildPathWithDelays(currentTask, hopDelays);
                }
            }
        }
        
        // Fallback to stored hops from logHop method
        if (taskHops.containsKey(taskId)) {
            String actualHops = taskHops.get(taskId).toString();
            if (!actualHops.trim().isEmpty()) {
                return actualHops.replaceAll(", $", ""); // Remove trailing comma
            }
        }
        
        // Last fallback to expected path
        return expectedPaths.getOrDefault(taskId, "Unknown path");
    }
    
    /**
     * Builds a path string with hop delays from the task's hop delay information
     */
    private String buildPathWithDelays(Task task, Map<Integer, Double> hopDelays) {
        // Build the actual PMU path with hop delays
        StringBuilder pathBuilder = new StringBuilder();
        
        // Check if we have the 6-hop PMU path (forward + return)
        if (hopDelays.containsKey(1) && hopDelays.containsKey(2) && hopDelays.containsKey(3)) {
            // Get the actual GNB name from stored mapping
            String gnbName = taskGnbNames.getOrDefault(task.getId(), "GNB");
            
            // Forward path: PMU -> GNB -> TELCO -> TSO
            pathBuilder.append(String.format("PMU->%s (%.4fs) -> TELCO (%.4fs) -> TSO (%.4fs)", 
                gnbName, hopDelays.get(1), hopDelays.get(2), hopDelays.get(3)));
            
            // Return path if available: TSO -> TELCO -> GNB -> PMU
            if (hopDelays.containsKey(4) && hopDelays.containsKey(5) && hopDelays.containsKey(6)) {
                pathBuilder.append(String.format(" -> TSO (%.4fs) -> TELCO (%.4fs) -> %s (%.4fs) -> PMU", 
                    hopDelays.get(4), hopDelays.get(5), gnbName, hopDelays.get(6)));
            }
            
            // Add total hop delay sum for comparison
            double totalHopDelay = hopDelays.values().stream().mapToDouble(Double::doubleValue).sum();
            pathBuilder.append(String.format(" [HopSum: %.4fs]", totalHopDelay));
            
        } else {
            // Fallback: simple hop enumeration
            for (Map.Entry<Integer, Double> entry : hopDelays.entrySet()) {
                pathBuilder.append(String.format("Hop%d (%.4fs) -> ", entry.getKey(), entry.getValue()));
            }
            if (pathBuilder.length() > 0) {
                pathBuilder.setLength(pathBuilder.length() - 4); // Remove last " -> "
            }
        }
        
        return pathBuilder.toString();
    }
    

    
    /**
     * Helper method to find a task by ID (this is a simplified version)
     */
    private Task findTaskById(int taskId) {
        // This is a simplified approach - in a real implementation you might need
        // to store task references or access them through the simulation manager
        // For now, we'll return null and rely on the hop delays being stored
        return null;
    }
    
    /**
     * Gets the actual network path for a task with hop delays
     */
    private String getTaskPathWithDelays(Task task) {
        // Get hop delays from the task
        Map<Integer, Double> hopDelays = task.getAllHopDelays();
        
        // Debug logging with more detail
        System.out.println(String.format("CloudLogger - Task %d hop delays: %s (size: %d)", task.getId(), hopDelays, hopDelays.size()));
        System.out.println(String.format("CloudLogger - Task %d network time: %.4f, waiting time: %.4f, execution time: %.4f", 
            task.getId(), task.getActualNetworkTime(), task.getWatingTime(), task.getActualCpuTime()));
        
        if (!hopDelays.isEmpty()) {
            // Build path with actual hop delays
            String pathWithDelays = buildPathWithDelays(task, hopDelays);
            System.out.println(String.format("CloudLogger - Task %d path with delays: %s", task.getId(), pathWithDelays));
            return pathWithDelays;
        }
        
        // Try to get stored hops first before falling back to expected path
        if (taskHops.containsKey(task.getId())) {
            String actualHops = taskHops.get(task.getId()).toString();
            if (!actualHops.trim().isEmpty()) {
                System.out.println(String.format("CloudLogger - Task %d using stored hops: %s", task.getId(), actualHops));
                return actualHops.replaceAll(", $", ""); // Remove trailing comma
            }
        }
        
        // Last fallback to expected path
        String fallbackPath = expectedPaths.getOrDefault(task.getId(), "No path recorded");
        System.out.println(String.format("CloudLogger - Task %d using fallback path: %s", task.getId(), fallbackPath));
        return fallbackPath;
    }
    
    /**
     * Extracts batch information from task description and formats it for logging
     */
    private String extractBatchInfo(Task task) {
        try {
            String taskType = task.getType();
            if (taskType != null && taskType.startsWith("BATCH:")) {
                // Format: "BATCH:windowTime:currentIndex/totalSize:batchType"
                String[] parts = taskType.split(":");
                if (parts.length >= 4) {
                    String windowTime = parts[1];
                    String indexInfo = parts[2];  // "X/Y" format
                    String batchType = parts[3];
                    
                    // Format as [X/Y] for display
                    return String.format("[%s]", indexInfo);
                }
            }
        } catch (Exception e) {
            // If parsing fails, return default batch info
        }
        
        // Default batch info for tasks without batch information
        return "[?/" + totalPmuCount + "]";
    }
    
    /**
     * Flushes all log writers to ensure data is written immediately
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
     * Logs individual PMU data transfer with full details (as requested by user)
     */
    public void logPmuDataTransfer(int pmuId, double dataSizeKB, double currentTime, double timeWindow) {
        // This will be replaced with detailed logging when we have the actual task
        // For now, store the basic info to be enhanced later
        String message = String.format("%.4f | PMU_%d → TSO: %.2f KB | Window: %.1f", 
                                     currentTime, pmuId, dataSizeKB, timeWindow);
        log(message);
        
        // Also print to console for immediate visibility
        System.out.println(message);
        
        // Flush immediately for real-time visibility
        flush();
    }
    
    /**
     * Logs PMU data transfer with full task details (new method)
     */
    public void logPmuDataTransferFull(Task dataTask, int pmuId, double dataSizeKB, String path) {
        // Call the new method with deadlineMissed = false and hopSum = 0.0 for backward compatibility
        logPmuDataTransferFull(dataTask, pmuId, dataSizeKB, path, false, 0.0);
    }
    
    /**
     * **NEW: Logs PMU data transfer with full task details and deadline missed flag**
     */
    public void logPmuDataTransferFull(Task dataTask, int pmuId, double dataSizeKB, String path, boolean deadlineMissed) {
        // Call with hopSum = 0.0 for backward compatibility
        logPmuDataTransferFull(dataTask, pmuId, dataSizeKB, path, deadlineMissed, 0.0);
    }
    
    /**
     * **NEW: Logs PMU data transfer with full task details, deadline missed flag, and hop sum**
     */
    public void logPmuDataTransferFull(Task dataTask, int pmuId, double dataSizeKB, String path, boolean deadlineMissed, double hopSum) {
        // **CHANGED: Use task generation time instead of simulation clock**
        double generationTime = dataTask.getTime(); 
        
        // Get PMU location
        ComputingNode pmu = dataTask.getEdgeDevice();
        Location pmuLocation = pmu != null ? pmu.getMobilityModel().getCurrentLocation() : new Location(0, 0);
        
        // Get destination info
        ComputingNode destination = dataTask.getOffloadingDestination();
        String destinationName = destination != null && destination.getName() != null && !destination.getName().trim().isEmpty() 
                                ? destination.getName() : "TSO";
        
        // Calculate return size (estimation)
        double returnSizeKB = dataTask.getOutputSizeInBits() / 8192.0;
        
        // **NEW: Add deadline missed flag and hop sum to the log message**
        String deadlineFlag = deadlineMissed ? " [DEADLINE_MISSED]" : "";
        String hopSumInfo = hopSum > 0.0 ? String.format(" | Hop_Sum: %.4fs", hopSum) : "";
        
        // Create the PMU data transfer log entry with fixed-width formatting and hop sum
        String taskMessage = String.format(
            "%8.4f | PMU: %2d [%2d/%d] | Location (%7.1f,%7.1f) || Data ID: %6d | Data Size: %6.2f KB | Return: %6.2f KB || " +
            "Source: PMU | Orchestrator: PMU | Destination: %s || Path: %s%s |%s",
            generationTime, // **CHANGED: Now uses generation time**
            pmuId,
            pmuId + 1, // [1/14] format
            totalPmuCount,
            pmuLocation.getXPos(),
            pmuLocation.getYPos(),
            dataTask.getId(),
            dataSizeKB,
            returnSizeKB,
            destinationName,
            path.isEmpty() ? "PMU_" + pmuId + " -> GNB_? -> TELCO -> TSO" : path,
            hopSumInfo,
            deadlineFlag
        );
        
        // Use the custom time printing method
        printWithCustomTime(taskMessage);
        
        // Write to log file
        if (logWriter != null) {
            logWriter.println(taskMessage);
            logWriter.flush();
        }
        
        // **NEW: Add to CSV records with deadline missed marker and hop sum**
        String statusValue = deadlineMissed ? "DEADLINE_MISSED" : "OK";
        String csvRecord = String.format("%.4f,%d,\"(%.1f,%.1f)\",%.2f,\"%s\",%.4f,%s",
            generationTime, pmuId, pmuLocation.getXPos(), pmuLocation.getYPos(), dataSizeKB, path, hopSum, statusValue); // **CHANGED: CSV also uses generation time**
        csvRecords.add(csvRecord);
    }
    
    /**
     * Logs state estimation task completion with success/fail status
     */
    public void logStateEstimationTaskCompletion(Task stateTask, boolean success) {
        double currentTime = simulationManager != null ? simulationManager.getSimulation().clock() : 0.0;
        
        // Get TSO location  
        ComputingNode tso = stateTask.getOffloadingDestination();
        Location tsoLocation = tso != null ? tso.getMobilityModel().getCurrentLocation() : new Location(0, 0);
        
        // Calculate sizes
        double dataSizeKB = stateTask.getFileSizeInBits() / 8192.0;
        double returnSizeKB = stateTask.getOutputSizeInBits() / 8192.0;
        
        // Get timing information
        double waitingTime = stateTask.getWatingTime();
        double executionTime = stateTask.getActualCpuTime();
        double networkTime = stateTask.getActualNetworkTime();
        double totalTime = stateTask.getTotalDelay();
        
        // Parse window and coverage from task type
        String taskType = stateTask.getType();
        String windowInfo = "?";
        String coverageInfo = "[?/" + totalPmuCount + "]";
        String batchType = "UNKNOWN";
        
        if (taskType != null && taskType.startsWith("GRID_ANALYSIS:")) {
            String[] parts = taskType.split(":");
            if (parts.length >= 4) {
                windowInfo = parts[1];
                coverageInfo = "[" + parts[2] + "]";
                batchType = parts[3];
            }
        }
        
        // Status
        String status = success ? "S" : "F";
        
        // **NEW: Get PDC waiting time for this task**
        double pdcWaitingTime = taskCollectionTimes.getOrDefault(stateTask.getId(), 0.0);
        
        // **NEW: Add to State Estimation CSV**
        String stateEstimationCsvRecord = String.format("%.4f,%d,%s,%s,%s,%.2f,%.2f,%.2f,%.0f,%.4f,%.4f,%.4f,%.4f,%s,%.4f,%d",
            currentTime,
            stateTask.getId(),
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
            totalTime,
            status,
            pdcWaitingTime,
            success ? 1 : 0  // Success flag as integer for easier analysis
        );
        stateEstimationCsvRecords.add(stateEstimationCsvRecord);
        
        // Create the state estimation task completion log entry
        String taskMessage = String.format(
            "%8.4f | TSO: State_Est %s | Location (%7.1f,%7.1f) || Task ID: %6d | Data Size: %6.2f KB | Return: %6.2f KB | MaxLatency: %5.2f s | Length: %5.0f MI || " +
            "Source: TSO | Orchestrator: TSO | Destination: TSO || Wait: %7.4fs | Exec: %7.4fs | Net: %7.4fs | Total: %7.4fs || Status: [%s] || Window: %s | PDC Waiting: %6.4fs |",
            currentTime,
            coverageInfo,
            tsoLocation.getXPos(),
            tsoLocation.getYPos(),
            stateTask.getId(),
            dataSizeKB,
            returnSizeKB,
            stateTask.getMaxLatency(),
            stateTask.getLength(),
            waitingTime,
            executionTime,
            networkTime,
            totalTime,
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
    public void logStateEstimationTask(int taskId, double timeWindow, int pmuCount, 
                                     int totalPmus, String batchType, double totalDataKB) {
        // Simple notification that state estimation task was created
        String message = String.format("State Estimation Task %d created: Window %.1f, Coverage %d/%d (%s), Data %.2f KB", 
                                     taskId, timeWindow, pmuCount, totalPmus, batchType, totalDataKB);
        log(message);
        flush();
    }
    
    /**
     * **NEW: Logs state estimation task creation with PDC waiting timing**
     */
    public void logStateEstimationTaskWithTiming(int taskId, double timeWindow, int pmuCount, 
                                               int totalPmus, String batchType, double totalDataKB, double pdcWaitingTime) {
        // Enhanced notification with PDC waiting time
        String message = String.format("State Estimation Task %d created: Window %.1f, Coverage %d/%d (%s), Data %.2f KB, PDC Waiting Time: %.4fs", 
                                     taskId, timeWindow, pmuCount, totalPmus, batchType, totalDataKB, pdcWaitingTime);
        log(message);
        flush();
        
        // **NEW: Store PDC waiting time for this task**
        taskCollectionTimes.put(taskId, pdcWaitingTime);
    }
    
    /**
     * Logs when a data collection window completes - SIMPLIFIED
     */
    public void logDataCollectionComplete(double generationTime, int pmuCount, boolean isComplete) {
        String batchType = isComplete ? "COMPLETE" : "TIMEOUT";
        String message = String.format(">>> Data Collection for Generation Time %.1f: %d/%d PMUs [%s] <<<", 
                                     generationTime, pmuCount, totalPmuCount, batchType);
        log(message);
        flush();
    }
    
    /**
     * Stores hop times for a task from the network model
     */
    public void storeTaskHopTimes(int taskId, String gnbName, double pmuToGnbTime, double gnbToTelcoTime, double telcoToTsoTime) {
        taskHopTimes.put(taskId, new TaskHopInfo(gnbName, pmuToGnbTime, gnbToTelcoTime, telcoToTsoTime));
    }
    
    /**
     * Gets stored hop times for a task
     */
    public TaskHopInfo getTaskHopTimes(int taskId) {
        return taskHopTimes.get(taskId);
    }
    
    /**
     * Builds realistic path string with actual hop times
     */
    public String buildRealisticPath(int taskId, int pmuId) {
        TaskHopInfo hopInfo = taskHopTimes.get(taskId);
        
        if (hopInfo != null) {
            // Build path with actual hop times
            return String.format("PMU_%d -> %s (%.4fs) -> TELCO (%.4fs) -> TSO (%.4fs)", 
                               pmuId, hopInfo.gnbName, hopInfo.pmuToGnbTime, 
                               hopInfo.gnbToTelcoTime, hopInfo.telcoToTsoTime);
        } else {
            // Fallback to template
            return String.format("PMU_%d -> GNB_? (?.??s) -> TELCO (?.??s) -> TSO (?.??s)", pmuId);
        }
    }
    
    // **NEW: Helper method for State Estimation CSV file name**
    private String getStateEstimationCSVFileName() {
        String baseFileName = simulationManager.getSimulationLogger().getFileName("");
        // Add "state_estimation_" suffix before the .csv extension
        return baseFileName + "_state_estimation.csv";
    }
    
    /**
     * **NEW: Logs grid analysis task creation for dynamic collection**
     */
    public void logStateEstimationTaskCreation(Task analysisTask, int collectionId, int pmuCount, 
                                             int totalPmus, double pdcWaitingTime, double totalDataKB, String batchType) {
        // Enhanced notification with collection ID and PDC waiting time
        String message = String.format("Grid Analysis Task %d created: Collection %d, Coverage %d/%d (%s), Data %.2f KB, PDC Waiting Time: %.4fs", 
                                     analysisTask.getId(), collectionId, pmuCount, totalPmus, batchType, totalDataKB, pdcWaitingTime);
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
     * Log PMU network transfer using NetworkTransferResult from CloudNetworkModel
     */
    public void logPmuNetworkTransfer(com.mechalikh.pureedgesim.taskgenerator.Task dataTask, 
                                    UpfOnTelco_PdcOnCloud.CloudNetworkModel.NetworkTransferResult transferResult) {
        try {
            // Extract PMU ID from task
            int pmuId = extractPmuIdFromTask(dataTask);
            
            // Convert data size to KB
            double dataSizeKB = 2.0; // Fixed 2KB PMU data
            
            // Use the path string from transfer result
            String path = transferResult.getPathString();
            
            // Log the transfer with all details
            logPmuDataTransferFull(dataTask, pmuId, dataSizeKB, path, false, transferResult.totalDelay);
            
        } catch (Exception e) {
            System.err.println("CloudLogger - Error logging network transfer: " + e.getMessage());
        }
    }
    
    /**
     * Extract PMU ID from task (helper method)
     */
    private int extractPmuIdFromTask(com.mechalikh.pureedgesim.taskgenerator.Task task) {
        try {
            // Try to extract from edge device first
            if (task.getEdgeDevice() != null) {
                String deviceName = task.getEdgeDevice().getName();
                if (deviceName != null && deviceName.contains("_")) {
                    return Integer.parseInt(deviceName.split("_")[1]);
                }
            }
            
            // Fallback: use task ID modulo
            return (int)(task.getId() % 20); // Assuming max 20 PMUs
            
        } catch (Exception e) {
            return 0; // Default fallback
        }
    }
} 