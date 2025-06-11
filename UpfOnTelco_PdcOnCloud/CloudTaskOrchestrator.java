package UpfOnTelco_PdcOnCloud;

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
public class CloudTaskOrchestrator extends DefaultOrchestrator {
    
    // Data collection configuration
    private final int TOTAL_PMU_COUNT;
    private CloudDataCollectorDynamic dataCollector;
    private ComputingNode tsoNode;
    
    // Statistics
    private int totalDataTransfers = 0;
    private int totalStateEstimationTasks = 0;
    
    public CloudTaskOrchestrator(SimulationManager simulationManager) {
        super(simulationManager);
        
        // **Read PMU count dynamically from simulation parameters**
        this.TOTAL_PMU_COUNT = SimulationParameters.maxNumberOfEdgeDevices;
        
        this.algorithmName = "PMU_DYNAMIC_ORCHESTRATOR";
        
        System.out.println("CloudTaskOrchestrator - Initialized for DYNAMIC collection:");
        System.out.println("  - Dynamic window PMU measurement collection");
        System.out.println("  - Centralized state estimation at TSO after each window");
        System.out.println("  - Expected PMUs: " + TOTAL_PMU_COUNT);
    }
    
    @Override
    public void initialize() {
        super.initialize();
        
        // Find TSO cloud datacenter
        tsoNode = findTsoCloudDatacenter();
        
        if (tsoNode != ComputingNode.NULL) {
            // Initialize DYNAMIC data collector at TSO if not already done
            if (dataCollector == null) {
                dataCollector = new CloudDataCollectorDynamic(simulationManager, tsoNode);
                if (simulationManager instanceof CloudSimulationManager) {
                    ((CloudSimulationManager) simulationManager).setDataCollector(dataCollector);
                }
            }
            
            // Log initialization
            simulationManager.getSimulationLogger().print("CloudSimulationManager - DYNAMIC data collector initialized for TSO: " + tsoNode.getName());
            simulationManager.getSimulationLogger().print("CloudSimulationManager - Initialized for DYNAMIC collection:");
            simulationManager.getSimulationLogger().print("  - Dynamic window (0.5s from first arrival)");
            simulationManager.getSimulationLogger().print("  - Centralized state estimation at TSO");
            simulationManager.getSimulationLogger().print("  - Expected PMUs: " + 
                simulationManager.getDataCentersManager().getComputingNodesGenerator().getMistOnlyList().size());
        } else {
            simulationManager.getSimulationLogger().print("CloudSimulationManager - ERROR: No TSO node found!");
        }
    }
    
    @Override
    public void orchestrate(Task task) {
        // **DEBUG: Log every task that arrives**
        double taskTime = task.getTime();
        int taskId = (int)task.getId();
        System.out.printf("CloudSimulationManager - DEBUG: Received task ID %d at generation time %.3f%n", taskId, taskTime);
        
        // **CRITICAL: Skip State Estimation tasks to prevent infinite loop**
        String taskType = task.getType();
        ComputingNode sourceDevice = task.getEdgeDevice();
        
        // **ENHANCED: Multiple checks for Grid Analysis tasks**
        boolean isGridAnalysisTask = (taskType != null && taskType.startsWith("GRID_ANALYSIS:")) ||
                                      (sourceDevice != null && sourceDevice.getName() != null && sourceDevice.getName().equals("TSO")) ||
                                      (task.getOffloadingDestination() != null && task.getOffloadingDestination().equals(tsoNode)) ||
                                      (task.getId() >= 10000); // All Grid Analysis tasks have ID >= 10000
        
        if (isGridAnalysisTask) {
            // **DO NOT PROCESS Grid Analysis tasks in orchestrate() - they should be handled differently**
            return; // **CRITICAL: Exit early to prevent loop**
        }
        
        // **ADDITIONAL SAFETY CHECK: Skip tasks from TSO location**
        if (sourceDevice != null) {
            Location loc = sourceDevice.getMobilityModel().getCurrentLocation();
            if (loc.getXPos() == -1.0 && loc.getYPos() == -1.0) {
                // This task comes from TSO location - it's likely a State Estimation task
                return; // **CRITICAL: Exit early to prevent loop**
            }
        }
        
        // **ONLY PROCESS genuine PMU data tasks**
        if (dataCollector != null) {
            // Forward the genuine PMU measurement data to dynamic collector
            dataCollector.collectPmuData(task);
        } else {
            simulationManager.getSimulationLogger().print("CloudSimulationManager - ERROR: Dynamic data collector not initialized");
        }
    }
    
    /**
     * Finds the TSO cloud datacenter by name
     */
    private ComputingNode findTsoCloudDatacenter() {
        List<ComputingNode> cloudDatacenters = simulationManager.getDataCentersManager()
                .getComputingNodesGenerator().getCloudOnlyList();
        
        for (ComputingNode cloud : cloudDatacenters) {
            if (cloud.getName() != null && cloud.getName().equals("TSO")) {
                return cloud;
            }
        }
        
        // Fallback to first cloud datacenter
        if (!cloudDatacenters.isEmpty()) {
            System.out.println("CloudSimulationManager - TSO not found, using first cloud: " + 
                             cloudDatacenters.get(0).getName());
            return cloudDatacenters.get(0);
        }
        
        return ComputingNode.NULL;
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
        System.out.println("=== PMU Dynamic Orchestrator Statistics ===");
        System.out.println("Total Data Transfers: " + totalDataTransfers);
        System.out.println("Total State Estimation Tasks: " + totalStateEstimationTasks);
        
        if (dataCollector != null) {
            System.out.println(dataCollector.getStatistics());
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
        boolean isGridAnalysisTask = (taskType != null && taskType.startsWith("GRID_ANALYSIS:")) ||
                                      (sourceDevice != null && sourceDevice.getName() != null && sourceDevice.getName().equals("TSO")) ||
                                      (task.getOffloadingDestination() != null && task.getOffloadingDestination().equals(tsoNode)) ||
                                      (task.getId() >= 10000); // All Grid Analysis tasks have ID >= 10000
        
        if (isGridAnalysisTask) {
            // **HANDLE GRID ANALYSIS TASK COMPLETION - NO FURTHER PROCESSING**
            handleGridAnalysisComplete(task);
            
            // **Log grid analysis task completion with clear identification**
            boolean success = task.getStatus() == Task.Status.SUCCESS;
            simulationManager.getSimulationLogger().print(String.format(
                ">>> GRID ANALYSIS TASK COMPLETED <<<  ID: %d | TSO Location: (-1.0, -1.0) | Status: %s | Type: %s",
                task.getId(), success ? "SUCCESS" : "FAILED", taskType != null ? taskType : "GRID_ANALYSIS"));
            CloudLogger.getInstance().logGridAnalysisTaskCompletion(task, success);
            
            // **CRITICAL: DO NOT CALL super.resultsReturned() for Grid Analysis tasks!**
            // This prevents the infinite loop where completed Grid Analysis tasks 
            // are treated as new PMU data tasks
            
        } else {
            // **HANDLE PMU DATA TASK COMPLETION**
            
            // **ADDITIONAL SAFETY CHECK: Skip tasks from TSO location**
            if (sourceDevice != null) {
                Location loc = sourceDevice.getMobilityModel().getCurrentLocation();
                if (loc.getXPos() == -1.0 && loc.getYPos() == -1.0) {
                    // This task comes from TSO location - it's likely a mis-classified Grid Analysis task
                    simulationManager.getSimulationLogger().print(String.format(
                        ">>> WARNING: Skipping task from TSO location <<<  ID: %d | Type: %s | Source: %s",
                        task.getId(), taskType, sourceDevice.getName()));
                    return;
                }
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
        System.out.printf("CloudSimulationManager - Grid analysis completed: %s at time %.4f%n", 
                         taskType, simulationManager.getSimulation().clock());
        
        // Parse task information
        try {
            // Format: "GRID_ANALYSIS:Collection_X:pmuCount/totalPmus:batchType"
            String[] parts = taskType.split(":");
            if (parts.length >= 4) {
                String collectionInfo = parts[1];
                String countInfo = parts[2];
                String batchType = parts[3];
                
                System.out.printf("CloudSimulationManager - Grid analysis for %s: %s data (%s)%n", 
                                 collectionInfo, countInfo, batchType);
            }
        } catch (Exception e) {
            System.err.println("CloudSimulationManager - Error parsing grid analysis task: " + e.getMessage());
        }
    }
    
    /**
     * Gets current orchestrator status for monitoring
     */
    public String getOrchestratorStatus() {
        if (dataCollector != null) {
            return String.format("PMU Dynamic Orchestrator: %d data transfers, %d state tasks - %s",
                               totalDataTransfers, totalStateEstimationTasks, dataCollector.getStatistics());
        }
        return "PMU Dynamic Orchestrator: Data collector not initialized";
    }
    
    /**
     * Gets the data collector instance
     */
    public CloudDataCollectorDynamic getDataCollector() {
        return dataCollector;
    }
    
    /**
     * Sets the data collector instance (for external initialization)
     */
    public void setDataCollector(CloudDataCollectorDynamic collector) {
        this.dataCollector = collector;
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