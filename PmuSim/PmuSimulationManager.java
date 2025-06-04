package PmuSim;

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
public class PmuSimulationManager extends DefaultSimulationManager implements OnSimulationEndListener {
    
    private PmuLogger pmuLogger;
    private PmuDataCollectorDynamic dataCollector;
    private ComputingNode tsoNode;
    private static final String OUTPUT_FOLDER = "PmuSim/Pmu_output/";
    
    public PmuSimulationManager(SimLog simLog, PureEdgeSim pureEdgeSim, int simulationId, int iteration,
            Scenario scenario) {
        super(simLog, pureEdgeSim, simulationId, iteration, scenario);
        
        try {
            this.pmuLogger = PmuLogger.initialize(this, OUTPUT_FOLDER);
            simLog.println("PMU Logger initialized successfully");
        } catch (Exception e) {
            simLog.println("Failed to initialize PMU Logger: " + e.getMessage());
        }
        
        // Set the custom PMU Network Model for data-based transfers
        setNetworkModel(new PmuNetworkModel(this));
        
        simLog.println("PmuSimulationManager initialized with DYNAMIC collection windows");
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
        if (pmuLogger != null) {
            pmuLogger.log("=== PMU Smart Grid DYNAMIC Collection Simulation Started ===");
            pmuLogger.log("Collection Mode: DYNAMIC WINDOWS (starts with first data arrival)");
            pmuLogger.log("Collection Window: 0.5 seconds from first PMU data");
            pmuLogger.log("Orchestrator: PmuTaskOrchestrator (Dynamic Data Collection)");
            pmuLogger.log("Network Model: PmuNetworkModel (Data-based transfers)");
            pmuLogger.log("Architecture: PMUs send measurement data → TSO waits for window → Grid analysis");
            pmuLogger.log("Edge Devices (PMUs): %d", 
                getDataCentersManager().getComputingNodesGenerator().getMistOnlyList().size());
            pmuLogger.log("Edge Datacenters: %d", 
                getDataCentersManager().getComputingNodesGenerator().getEdgeOnlyList().size());
            pmuLogger.log("TSO Cloud: %s", tsoNode.getName());
            pmuLogger.flush();
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
    public PmuDataCollectorDynamic getDataCollector() {
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
            this.dataCollector = new PmuDataCollectorDynamic(this, tsoNode);
            if (edgeOrchestrator instanceof PmuTaskOrchestrator) {
                ((PmuTaskOrchestrator) edgeOrchestrator).setDataCollector(dataCollector);
            }
            simLog.println("PMU Dynamic Data Collector initialized for TSO: " + tsoNode.getName());
        } else {
            simLog.println("WARNING - No TSO node found for data collector!");
        }
    }
    
    /**
     * Sets the PMU data collector instance
     */
    public void setDataCollector(PmuDataCollectorDynamic collector) {
        this.dataCollector = collector;
    }
    
    /**
     * Gets the PMU logger instance
     */
    public PmuLogger getPmuLogger() {
        return pmuLogger;
    }
    
    /**
     * Gets data orchestrator status for monitoring
     */
    public String getDataOrchestratorStatus() {
        if (edgeOrchestrator instanceof PmuTaskOrchestrator) {
            return ((PmuTaskOrchestrator) edgeOrchestrator).getOrchestratorStatus();
        }
        return "Data orchestrator not active";
    }

    public void processEvent(com.mechalikh.pureedgesim.simulationengine.Event ev) {
        // Handle custom PMU events first
        switch (ev.getTag()) {
            case 300: // PMU_DATA_RECEIVED
                // Forward to data collector if it exists
                if (edgeOrchestrator instanceof PmuTaskOrchestrator) {
                    PmuTaskOrchestrator orchestrator = (PmuTaskOrchestrator) edgeOrchestrator;
                    // Get the data collector and forward the event
                    if (orchestrator.getDataCollector() != null) {
                        com.mechalikh.pureedgesim.taskgenerator.Task task = 
                            (com.mechalikh.pureedgesim.taskgenerator.Task) ev.getData();
                        simLog.println("PmuSimulationManager - Forwarding PMU data to DYNAMIC collector for PMU: " + 
                                     (task.getEdgeDevice() != null ? task.getEdgeDevice().getName() : "unknown"));
                        
                        // Forward to data collector
                        orchestrator.getDataCollector().processEvent(ev);
                    } else {
                        simLog.println("PmuSimulationManager - Dynamic data collector not available");
                    }
                } else {
                    simLog.println("PmuSimulationManager - PMU orchestrator not available");
                }
                break;
            case 301: // COLLECTION_TIMEOUT
                // Forward to data collector if it exists
                if (edgeOrchestrator instanceof PmuTaskOrchestrator) {
                    PmuTaskOrchestrator orchestrator = (PmuTaskOrchestrator) edgeOrchestrator;
                    if (orchestrator.getDataCollector() != null) {
                        simLog.println("PmuSimulationManager - Forwarding COLLECTION_TIMEOUT event");
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
            if (pmuLogger != null) {
                // **Log final statistics for dynamic collection**
                if (dataCollector != null) {
                    pmuLogger.log("=== PMU Dynamic Collection Simulation Summary ===");
                    pmuLogger.log(dataCollector.getStatistics());
                }
                
                pmuLogger.log("PMU Dynamic Collection Simulation completed successfully");
                pmuLogger.close();
            }
            
            simLog.println("PMU Dynamic Collection Simulation ended - Check logs in: " + OUTPUT_FOLDER);
            
        } catch (Exception e) {
            simLog.println("Error during PMU simulation cleanup: " + e.getMessage());
            e.printStackTrace();
        }
    }
} 