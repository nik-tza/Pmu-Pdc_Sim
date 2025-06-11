package UpfOnEdge_PdcOnEdge;

import com.mechalikh.pureedgesim.datacentersmanager.ComputingNode;
import com.mechalikh.pureedgesim.simulationmanager.SimulationManager;
import com.mechalikh.pureedgesim.taskgenerator.Task;
import com.mechalikh.pureedgesim.taskorchestrator.DefaultOrchestrator;
import com.mechalikh.pureedgesim.network.TransferProgress;
import com.mechalikh.pureedgesim.scenariomanager.SimulationParameters;
import com.mechalikh.pureedgesim.locationmanager.Location;

import java.util.*;

/**
 * PMU Task Orchestrator - Dynamic Collection approach
 * PMUs send measurement data (not tasks) to TSO for dynamic window collection
 * 
 * Smart Grid Logic:
 * - PMUs are sensors that send measurement data, not computation tasks
 * - TSO starts collection window with first arrival and waits for fixed period
 * - State estimation task is created after each collection window
 */
public class EdgeTaskOrchestrator extends DefaultOrchestrator {
    
    // Data collection configuration
    private final int TOTAL_PMU_COUNT;
    // NOTE: Data collectors are now managed by EdgeSimulationManager (distributed per-GNB)
    
    // Statistics
    private int totalDataTransfers = 0;
    private int totalStateEstimationTasks = 0;
    
    public EdgeTaskOrchestrator(SimulationManager simulationManager) {
        super(simulationManager);
        
        // **Read PMU count dynamically from simulation parameters**
        this.TOTAL_PMU_COUNT = SimulationParameters.maxNumberOfEdgeDevices;
        
        this.algorithmName = "PMU_DYNAMIC_ORCHESTRATOR";
        
        System.out.println("PmuTaskOrchestrator - Initialized for DISTRIBUTED GNB collection:");
        System.out.println("  - Dynamic window PMU measurement collection per GNB");
        System.out.println("  - Local grid analysis at each GNB after window completion");
        System.out.println("  - Expected PMUs: " + TOTAL_PMU_COUNT);
    }
    
    @Override
    public void initialize() {
        super.initialize();
        
        // Data collectors are initialized by EdgeSimulationManager during simulation start
        // No need to initialize them here anymore
        
        // Log initialization
        int gnbCount = getGnbCount();
        simulationManager.getSimulationLogger().print("PmuTaskOrchestrator - Initialized for DISTRIBUTED GNB collection:");
        simulationManager.getSimulationLogger().print("  - Dynamic window (0.045s from first arrival per GNB)");
        simulationManager.getSimulationLogger().print("  - Local grid analysis at each GNB");
        simulationManager.getSimulationLogger().print("  - Expected PMUs: " + 
            simulationManager.getDataCentersManager().getComputingNodesGenerator().getMistOnlyList().size());
        simulationManager.getSimulationLogger().print("  - GNBs with collectors: " + gnbCount);
    }
    
    @Override
    public synchronized void orchestrate(Task task) {
        // **DEBUG: Log every task that arrives**
        double taskTime = task.getTime();
        int taskId = (int)task.getId();
        System.out.printf("PmuTaskOrchestrator - DEBUG: Received task ID %d at generation time %.3f%n", taskId, taskTime);
        
        // **CRITICAL: Skip Grid Analysis tasks to prevent infinite loop**
        String taskType = task.getType();
        ComputingNode sourceDevice = task.getEdgeDevice();
        
        // **ENHANCED: Multiple checks for Grid Analysis tasks**
        boolean isGridAnalysisTask = (taskType != null && taskType.startsWith("GridAnalysisTask_")) ||
                                      (taskType != null && taskType.startsWith("GRID_ANALYSIS:")) ||
                                      (sourceDevice != null && sourceDevice.getName() != null && 
                                       (sourceDevice.getName().startsWith("EDGE_") || sourceDevice.getName().startsWith("Edge_"))) ||
                                      (task.getId() >= 10000); // All Grid Analysis tasks have ID >= 10000
        
        if (isGridAnalysisTask) {
            // **DO NOT PROCESS Grid Analysis tasks in orchestrate() - they are handled by GNB locally**
            System.out.printf("PmuTaskOrchestrator - Skipping Grid Analysis task ID %d (local GNB processing)%n", taskId);
            return; // **CRITICAL: Exit early to prevent loop**
        }
        
        // **ADDITIONAL SAFETY CHECK: Skip tasks from GNB locations**
        if (sourceDevice != null && sourceDevice.getName() != null && 
            (sourceDevice.getName().startsWith("EDGE_") || sourceDevice.getName().startsWith("Edge_"))) {
            // This task comes from GNB - it's likely a Grid Analysis task
            System.out.printf("PmuTaskOrchestrator - Skipping task from GNB %s (ID: %d)%n", sourceDevice.getName(), taskId);
            return; // **CRITICAL: Exit early to prevent loop**
        }
        
        // **ONLY PROCESS genuine PMU data tasks**
        if (simulationManager instanceof EdgeSimulationManager) {
            EdgeSimulationManager edgeManager = (EdgeSimulationManager) simulationManager;
            
            // Find the appropriate data collector for this PMU
            EdgeDataCollectorDynamic collector = edgeManager.getDataCollectorForPmu(sourceDevice);
            
            if (collector != null) {
                // Forward the genuine PMU measurement data to the appropriate GNB collector
                collector.collectEdgeData(task);
                System.out.printf("PmuTaskOrchestrator - Forwarded PMU data ID %d to GNB collector%n", taskId);
            } else {
                simulationManager.getSimulationLogger().print("PmuTaskOrchestrator - ERROR: No data collector found for PMU: " + 
                    (sourceDevice != null ? sourceDevice.getName() : "unknown"));
            }
        } else {
            simulationManager.getSimulationLogger().print("PmuTaskOrchestrator - ERROR: SimulationManager is not EdgeSimulationManager");
        }
    }
    
    /**
     * Gets the number of GNBs in the simulation
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
            return 0;
        }
    }
    
    /**
     * Extracts PMU ID from task's edge device
     */
    private int extractPmuId(ComputingNode pmuDevice) {
        try {
            String deviceName = pmuDevice.getName();
            if (deviceName != null && deviceName.startsWith("PMU_")) {
                return Integer.parseInt(deviceName.substring(4));
            }
        } catch (NumberFormatException e) {
            // Fall back to finding index
        }
        
        // Fallback: find device index in the device list
        List<ComputingNode> devices = simulationManager.getDataCentersManager()
                .getComputingNodesGenerator().getMistOnlyList();
        
        for (int i = 0; i < devices.size(); i++) {
            if (devices.get(i).equals(pmuDevice)) {
                return i; // PMU IDs start from 0
            }
        }
        
        return 0; // Unknown PMU
    }
    
    /**
     * Logs orchestrator statistics
     */
    private void logStatistics() {
        System.out.println("=== PMU Distributed GNB Orchestrator Statistics ===");
        System.out.println("Total Data Transfers: " + totalDataTransfers);
        System.out.println("Total Grid Analysis Tasks: " + totalStateEstimationTasks);
        
        // Log statistics from GNB data collectors
        if (simulationManager instanceof EdgeSimulationManager) {
            EdgeSimulationManager edgeManager = (EdgeSimulationManager) simulationManager;
            System.out.println("GNB Data Collectors Status:");
            // No longer using central collector - distributed approach now
        }
        
        double dataTransfersPerSecond = totalDataTransfers / Math.max(1, simulationManager.getSimulation().clock());
        System.out.printf("Data Transfer Rate: %.2f transfers/second%n", dataTransfersPerSecond);
        System.out.println("===============================================");
    }
    
    @Override
    public void resultsReturned(Task task) {
        String taskType = task.getType();
        ComputingNode sourceDevice = task.getEdgeDevice();
        
        // **ENHANCED: Multiple checks for Grid Analysis tasks**
        boolean isGridAnalysisTask = (taskType != null && taskType.startsWith("GridAnalysisTask_")) ||
                                      (taskType != null && taskType.startsWith("GRID_ANALYSIS:")) ||
                                      (sourceDevice != null && sourceDevice.getName() != null && 
                                       (sourceDevice.getName().startsWith("EDGE_") || sourceDevice.getName().startsWith("Edge_"))) ||
                                      (task.getId() >= 10000); // All Grid Analysis tasks have ID >= 10000
        
        if (isGridAnalysisTask) {
            // **HANDLE GRID ANALYSIS TASK COMPLETION - NO FURTHER PROCESSING**
            handleGridAnalysisComplete(task);
            
            // **Log grid analysis task completion with clear identification**
            boolean success = task.getStatus() == Task.Status.SUCCESS;
            String gnbName = sourceDevice != null ? sourceDevice.getName() : "Unknown_GNB";
            simulationManager.getSimulationLogger().print(String.format(
                ">>> GRID ANALYSIS TASK COMPLETED <<<  ID: %d | GNB: %s | Status: %s | Type: %s",
                task.getId(), gnbName, success ? "SUCCESS" : "FAILED", taskType != null ? taskType : "GridAnalysisTask"));
            EdgeLogger.getInstance().logGridAnalysisTaskCompletion(task, success);
            
            // **CRITICAL: DO NOT CALL super.resultsReturned() for Grid Analysis tasks!**
            // This prevents the infinite loop where completed Grid Analysis tasks 
            // are treated as new PMU data tasks
            
        } else {
            // **HANDLE PMU DATA TASK COMPLETION**
            
            // **ADDITIONAL SAFETY CHECK: Skip tasks from GNB locations (they're Grid Analysis tasks)**
            if (sourceDevice != null && sourceDevice.getName() != null && 
                (sourceDevice.getName().startsWith("EDGE_") || sourceDevice.getName().startsWith("Edge_"))) {
                // This task comes from GNB location - it's likely a mis-classified Grid Analysis task
                simulationManager.getSimulationLogger().print(String.format(
                    ">>> WARNING: Skipping task from GNB location <<<  ID: %d | Type: %s | Source: %s",
                    task.getId(), taskType, sourceDevice.getName()));
                return;
            }
            
            // **NEW: Log PMU task with generation time (like DroneSim)**
            logPmuTaskCompletion(task);
            
            // Call parent implementation for genuine PMU data tasks
            super.resultsReturned(task);
            
            // **Log PMU data task completion with clear identification**
            String deviceLocation = sourceDevice != null ? 
                String.format("(%.1f, %.1f)", 
                    sourceDevice.getMobilityModel().getCurrentLocation().getXPos(),
                    sourceDevice.getMobilityModel().getCurrentLocation().getYPos()) : "(?,?)";
            String deviceName = sourceDevice != null ? sourceDevice.getName() : "Unknown";
            
            simulationManager.getSimulationLogger().print(String.format(
                ">>> PMU DATA TASK COMPLETED <<<  ID: %d | PMU Location: %s | Device: %s | Status: %s",
                task.getId(), deviceLocation, deviceName, 
                task.getStatus() == Task.Status.SUCCESS ? "SUCCESS" : "FAILED"));
        }
    }
    
    /**
     * Handles completion of grid analysis task
     */
    private void handleGridAnalysisComplete(Task task) {
        totalStateEstimationTasks++;
        
        String taskType = task.getType();
        ComputingNode gnb = task.getEdgeDevice();
        String gnbName = gnb != null ? gnb.getName() : "Unknown_GNB";
        
        System.out.printf("PmuTaskOrchestrator - Grid analysis completed: %s on %s at time %.4f%n", 
                         taskType, gnbName, simulationManager.getSimulation().clock());
        
        // Parse task information for new format
        try {
            // New format: "GridAnalysisTask_X:Window_Y:Z/W:TYPE"
            if (taskType != null && taskType.startsWith("GridAnalysisTask_")) {
                String[] parts = taskType.split(":");
                if (parts.length >= 4) {
                    String taskIdentifier = parts[0]; // GridAnalysisTask_X
                    String windowInfo = parts[1];     // Window_Y
                    String countInfo = parts[2];      // Z/W
                    String batchType = parts[3];      // TYPE
                    
                    System.out.printf("PmuTaskOrchestrator - %s on %s: %s for %s (%s)%n", 
                                     taskIdentifier, gnbName, windowInfo, countInfo, batchType);
                }
            }
        } catch (Exception e) {
            System.err.println("PmuTaskOrchestrator - Error parsing grid analysis task: " + e.getMessage());
        }
    }
    
    /**
     * Gets current orchestrator status for monitoring
     */
    public String getOrchestratorStatus() {
        if (simulationManager instanceof EdgeSimulationManager) {
            EdgeSimulationManager edgeManager = (EdgeSimulationManager) simulationManager;
            int gnbCount = getGnbCount();
            
            return String.format("Edge Distributed GNB Orchestrator: %d data transfers, %d grid analysis tasks - Distributed collectors (%d GNBs)",
                               totalDataTransfers, totalStateEstimationTasks, gnbCount);
        }
        return "Edge Distributed GNB Orchestrator: No simulation manager found";
    }
    
    /**
     * NEW: Logs PMU task completion with generation time (like DroneSim)
     */
    private void logPmuTaskCompletion(Task task) {
        if (simulationManager == null) return;
        
        // **KEY: Use task generation time instead of simulation clock**
        double generationTime = task.getTime(); // This shows fractional times like 0.333, 0.667
        String formattedTime = String.format("%.4f", generationTime);
        
        // Extract PMU information
        int pmuId = extractPmuId(task.getEdgeDevice());
        ComputingNode pmu = task.getEdgeDevice();
        Location pmuLocation = pmu != null ? pmu.getMobilityModel().getCurrentLocation() : new Location(0, 0);
        
        // Get task status
        String status = (task.getStatus() == Task.Status.SUCCESS) ? "SUCCESS" : "FAILED";
        
        // **Create message similar to DroneSim format**
        String taskMessage = String.format(
            "PMU: %d | Location (%.1f,%.1f) || Task ID: %6d | Status: %s",
            pmuId,
            pmuLocation.getXPos(),
            pmuLocation.getYPos(),
            task.getId(),
            status
        );
        
        // **Create formatted log message with generation time**
        String fullMessage = String.format("%s - generation time %s (s) : %s",
            new java.text.SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new java.util.Date()),
            formattedTime,
            taskMessage);
        
        // Log to file
        simulationManager.getSimulationLogger().printWithoutTime(fullMessage);
    }
} 