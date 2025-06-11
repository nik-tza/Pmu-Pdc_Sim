package UpfOnTelco_PdcOnCloud;

import com.mechalikh.pureedgesim.scenariomanager.Scenario;
import com.mechalikh.pureedgesim.scenariomanager.SimulationParameters;
import com.mechalikh.pureedgesim.simulationengine.PureEdgeSim;
import com.mechalikh.pureedgesim.simulationmanager.DefaultSimulationManager;
import com.mechalikh.pureedgesim.simulationmanager.SimLog;
import com.mechalikh.pureedgesim.simulationvisualizer.SimulationVisualizer;
import com.mechalikh.pureedgesim.taskgenerator.Task;
import com.mechalikh.pureedgesim.datacentersmanager.ComputingNode;
import com.mechalikh.pureedgesim.simulationengine.Event;
import com.mechalikh.pureedgesim.simulationengine.OnSimulationEndListener;
import java.util.List;

/**
 * PMU Simulation Manager - Dynamic Collection version
 * Manages PMU data collection with dynamic windows starting from first arrival
 */
public class CloudSimulationManager extends DefaultSimulationManager implements OnSimulationEndListener {
    
    private CloudLogger cloudLogger;
    private CloudDataCollectorDynamic dataCollector;
    private ComputingNode tsoNode;
    private static final String OUTPUT_FOLDER = "UpfOnTelco_PdcOnCloud/output/";
    
    public CloudSimulationManager(SimLog simLog, PureEdgeSim pureEdgeSim, int simulationId, int iteration,
            Scenario scenario) {
        super(simLog, pureEdgeSim, simulationId, iteration, scenario);
        
        try {
            this.cloudLogger = CloudLogger.initialize(this, OUTPUT_FOLDER);
            simLog.println("PMU Logger initialized successfully");
        } catch (Exception e) {
            simLog.println("Failed to initialize PMU Logger: " + e.getMessage());
        }
        
        // Set the custom Cloud Network Model for data-based transfers
        setNetworkModel(new CloudNetworkModel(this));
        
        simLog.println("CloudSimulationManager initialized with DYNAMIC collection windows");
    }
    
    @Override
    public void onSimulationStart() {
        try {
            // Call parent implementation first
            super.onSimulationStart();
            
            // **Initialize TSO node**
            this.tsoNode = findTsoNode();
            
            // Log PMU simulation specific information
            logPmuSimulationInfo();
            
            simLog.println("PMU Dynamic Collection Simulation started successfully");
            
        } catch (Exception e) {
            simLog.println("ERROR - Failed during PMU simulation start: " + e.getMessage());
            e.printStackTrace();
            simulation.terminate();
        }
    }
    
    /**
     * Finds the TSO node in cloud datacenters
     */
    private ComputingNode findTsoNode() {
        List<ComputingNode> cloudDatacenters = getDataCentersManager()
                .getComputingNodesGenerator().getCloudOnlyList();
        
        // First try to find TSO by name
        for (ComputingNode node : cloudDatacenters) {
            if (node.getName() != null && node.getName().equals("TSO")) {
                return node;
            }
        }
        
        // Fallback to first cloud datacenter
        if (!cloudDatacenters.isEmpty()) {
            simLog.println("Warning: TSO not found by name, using first cloud datacenter");
            return cloudDatacenters.get(0);
        }
        
        return null;
    }
    
    /**
     * Logs PMU simulation setup information
     */
    private void logPmuSimulationInfo() {
        if (cloudLogger != null) {
            cloudLogger.log("=== PMU Smart Grid DYNAMIC Collection Simulation Started ===");
            cloudLogger.log("Collection Mode: DYNAMIC WINDOWS (starts with first data arrival)");
            cloudLogger.log("Collection Window: 0.5 seconds from first PMU data");
            cloudLogger.log("Orchestrator: CloudTaskOrchestrator (Dynamic Data Collection)");
            cloudLogger.log("Network Model: CloudNetworkModel (Data-based transfers)");
            cloudLogger.log("Architecture: PMUs send measurement data → TSO waits for window → Grid analysis");
            cloudLogger.log("Edge Devices (PMUs): %d", 
                getDataCentersManager().getComputingNodesGenerator().getMistOnlyList().size());
            cloudLogger.log("Edge Datacenters: %d", 
                getDataCentersManager().getComputingNodesGenerator().getEdgeOnlyList().size());
            cloudLogger.log("TSO Cloud: %s", tsoNode.getName());
            cloudLogger.flush();
        }
        
        // Also log to simulation console
        simLog.println("PMU DYNAMIC Collection Simulation Configuration:");
        simLog.println("  - Collection Mode: DYNAMIC (0.5s window from first arrival)");
        simLog.println("  - Grid analysis: Centralized at TSO after each collection");
        simLog.println("  - Network Model: Data transfer simulation");
        simLog.println("  - Total PMUs: " + getDataCentersManager().getComputingNodesGenerator().getMistOnlyList().size());
    }
    
    /**
     * Gets the PMU data collector instance with lazy initialization
     */
    public CloudDataCollectorDynamic getDataCollector() {
        if (dataCollector == null) {
            initializeDataCollector();
        }
        return dataCollector;
    }
    
    /**
     * Initializes the data collector in a safe way (lazy initialization)
     */
    private void initializeDataCollector() {
        if (dataCollector != null) return; // Already initialized
        
        ComputingNode tsoNode = findTsoNode();
        if (tsoNode != null) {
            this.dataCollector = new CloudDataCollectorDynamic(this, tsoNode);
            if (edgeOrchestrator instanceof CloudTaskOrchestrator) {
                ((CloudTaskOrchestrator) edgeOrchestrator).setDataCollector(dataCollector);
            }
            simLog.println("PMU Dynamic Data Collector initialized for TSO: " + tsoNode.getName());
        } else {
            simLog.println("WARNING - No TSO node found for data collector!");
        }
    }
    
    /**
     * Sets the PMU data collector instance
     */
    public void setDataCollector(CloudDataCollectorDynamic collector) {
        this.dataCollector = collector;
    }
    
    /**
     * Gets the PMU logger instance
     */
    public CloudLogger getCloudLogger() {
        return cloudLogger;
    }
    
    /**
     * Gets data orchestrator status for monitoring
     */
    public String getDataOrchestratorStatus() {
        if (edgeOrchestrator instanceof CloudTaskOrchestrator) {
            return ((CloudTaskOrchestrator) edgeOrchestrator).getOrchestratorStatus();
        }
        return "Data orchestrator not active";
    }

    public void processEvent(com.mechalikh.pureedgesim.simulationengine.Event ev) {
        // Handle custom PMU events first
        switch (ev.getTag()) {
            case 300: // PMU_DATA_RECEIVED
                // Forward to data collector if it exists
                if (edgeOrchestrator instanceof CloudTaskOrchestrator) {
                    CloudTaskOrchestrator orchestrator = (CloudTaskOrchestrator) edgeOrchestrator;
                    // Get the data collector and forward the event
                    if (orchestrator.getDataCollector() != null) {
                        com.mechalikh.pureedgesim.taskgenerator.Task task = 
                            (com.mechalikh.pureedgesim.taskgenerator.Task) ev.getData();
                        simLog.println("CloudSimulationManager - Forwarding PMU data to DYNAMIC collector for PMU: " + 
                                     (task.getEdgeDevice() != null ? task.getEdgeDevice().getName() : "unknown"));
                        
                        // Forward to data collector
                        orchestrator.getDataCollector().processEvent(ev);
                    } else {
                        simLog.println("CloudSimulationManager - Dynamic data collector not available");
                    }
                } else {
                    simLog.println("CloudSimulationManager - PMU orchestrator not available");
                }
                break;
            case 301: // COLLECTION_TIMEOUT
                // Forward to data collector if it exists
                if (edgeOrchestrator instanceof CloudTaskOrchestrator) {
                    CloudTaskOrchestrator orchestrator = (CloudTaskOrchestrator) edgeOrchestrator;
                    if (orchestrator.getDataCollector() != null) {
                        simLog.println("CloudSimulationManager - Forwarding COLLECTION_TIMEOUT event");
                        orchestrator.getDataCollector().processEvent(ev);
                    }
                }
                break;
            default:
                // For other events, call parent implementation
                super.processEvent(ev);
                break;
        }
    }
    
    @Override
    public void onSimulationEnd() {
        try {
            if (cloudLogger != null) {
                // **Log final statistics for dynamic collection**
                if (dataCollector != null) {
                    cloudLogger.log("=== PMU Dynamic Collection Simulation Summary ===");
                    cloudLogger.log(dataCollector.getStatistics());
                }
                
                cloudLogger.log("PMU Dynamic Collection Simulation completed successfully");
                cloudLogger.close();
            }
            
            simLog.println("PMU Dynamic Collection Simulation ended - Check logs in: " + OUTPUT_FOLDER);
            
        } catch (Exception e) {
            simLog.println("Error during PMU simulation cleanup: " + e.getMessage());
            e.printStackTrace();
        }
    }
} 