package UpfOnEdge_PdcOnEdge;

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
import com.mechalikh.pureedgesim.simulationengine.OnSimulationStartListener;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;

/**
 * PMU Simulation Manager - Dynamic Collection version
 * Manages PMU data collection with dynamic windows starting from first arrival
 */
public class EdgeSimulationManager extends DefaultSimulationManager implements OnSimulationStartListener, OnSimulationEndListener {
    
    private EdgeLogger edgeLogger;
    private Map<ComputingNode, EdgeDataCollectorDynamic> gnbDataCollectors; // One collector per GNB
    private List<ComputingNode> gnbNodes; // List of all GNBs
    private static final String OUTPUT_FOLDER = "UpfOnEdge_PdcOnEdge/output/";
    
    public EdgeSimulationManager(SimLog simLog, PureEdgeSim pureEdgeSim, int simulationId, int iteration,
            Scenario scenario) {
        super(simLog, pureEdgeSim, simulationId, iteration, scenario);
        
        try {
            this.edgeLogger = EdgeLogger.initialize(this, OUTPUT_FOLDER);
            simLog.println("PMU Logger initialized successfully");
            
            // Initialize GNB data structures
            this.gnbNodes = new ArrayList<>();
            this.gnbDataCollectors = new HashMap<>();
            
        } catch (Exception e) {
            simLog.println("Failed to initialize PMU Logger: " + e.getMessage());
        }
        
        // Set the custom Edge Network Model for data-based transfers
        setNetworkModel(new EdgeNetworkModel(this));
        
        simLog.println("EdgeSimulationManager initialized with DISTRIBUTED GNB collection");
    }
    
    /**
     * Override startSimulation to initialize collector BEFORE simulation.start()
     * This avoids ConcurrentModificationException from creating entities during forEach
     */
    @Override
    public void startSimulation() {
        // Initialize GNBs and per-GNB collectors BEFORE starting simulation engine
        // This ensures all entities are created before PureEdgeSim.start() begins
        initializeGnbDataCollectors();
        
        // Now call parent to start the simulation engine
        super.startSimulation();
    }
    
    @Override
    public void onSimulationStart() {
        try {
            // Call parent implementation first
            super.onSimulationStart();
            
            // **Initialize GNBs and per-GNB collectors only if not already created**
            if (gnbDataCollectors.isEmpty()) {
                initializeGnbDataCollectors();
            }
            
            // Log PMU simulation specific information
            logPmuSimulationInfo();
            
            simLog.println("PMU Distributed GNB Collection Simulation started successfully");
            
        } catch (Exception e) {
            simLog.println("ERROR - Failed during PMU simulation start: " + e.getMessage());
            e.printStackTrace();
            simulation.terminate();
        }
    }
    
    /**
     * Initializes one data collector per GNB for distributed processing
     */
    private void initializeGnbDataCollectors() {
        try {
            // Get all edge datacenters (GNBs and TELCO)
            List<ComputingNode> edgeDatacenters = getDataCentersManager()
                    .getComputingNodesGenerator().getEdgeOnlyList();
            
            // Get all PMU devices
            List<ComputingNode> pmuDevices = getDataCentersManager()
                    .getComputingNodesGenerator().getMistOnlyList();
            
            // Filter out TELCO to get only GNBs
            for (ComputingNode edgeDatacenter : edgeDatacenters) {
                if (edgeDatacenter.getName() == null || !edgeDatacenter.getName().equals("TELCO")) {
                    gnbNodes.add(edgeDatacenter);
                }
            }
            
            simLog.println("Found " + gnbNodes.size() + " GNBs and " + pmuDevices.size() + " PMUs for distributed data collection");
            
            // Calculate PMU assignment for each GNB
            Map<ComputingNode, Integer> gnbPmuCounts = calculatePmuAssignments(gnbNodes, pmuDevices);
            
            // Create one EdgeDataCollectorDynamic per GNB with correct PMU count
            for (ComputingNode gnb : gnbNodes) {
                int expectedPmuCount = gnbPmuCounts.getOrDefault(gnb, 0);
                EdgeDataCollectorDynamic collector = new EdgeDataCollectorDynamic(this, gnb, expectedPmuCount);
                gnbDataCollectors.put(gnb, collector);
                simLog.println("Initialized data collector for " + gnb.getName() + 
                             " - Expecting " + expectedPmuCount + " PMUs");
            }
            
            simLog.println("Initialized " + gnbDataCollectors.size() + " distributed GNB data collectors");
            
        } catch (Exception e) {
            simLog.println("ERROR - Failed to initialize GNB data collectors: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Calculate how many PMUs are assigned to each GNB based on closest distance
     */
    private Map<ComputingNode, Integer> calculatePmuAssignments(List<ComputingNode> gnbNodes, List<ComputingNode> pmuDevices) {
        Map<ComputingNode, Integer> assignments = new HashMap<>();
        
        // Initialize counters
        for (ComputingNode gnb : gnbNodes) {
            assignments.put(gnb, 0);
        }
        
        // For each PMU, find closest GNB and increment counter
        for (ComputingNode pmu : pmuDevices) {
            ComputingNode closestGnb = findClosestGnbForPmu(pmu);
            if (closestGnb != null) {
                assignments.put(closestGnb, assignments.get(closestGnb) + 1);
            }
        }
        
        // Log assignments for debugging
        simLog.println("PMU assignments to GNBs:");
        for (Map.Entry<ComputingNode, Integer> entry : assignments.entrySet()) {
            simLog.println("  - " + entry.getKey().getName() + ": " + entry.getValue() + " PMUs");
        }
        
        return assignments;
    }
    
    /**
     * Logs PMU simulation setup information
     */
    private void logPmuSimulationInfo() {
        if (edgeLogger != null) {
            edgeLogger.log("=== Edge Smart Grid DISTRIBUTED GNB Collection Simulation Started ===");
            edgeLogger.log("Collection Mode: PER-GNB DYNAMIC WINDOWS (starts with first data arrival at each GNB)");
            edgeLogger.log("Collection Window: 0.045 seconds from first PMU data at each GNB");
            edgeLogger.log("Orchestrator: EdgeTaskOrchestrator (Distributed GNB Data Collection)");
            edgeLogger.log("Network Model: EdgeNetworkModel (PMU → GNB only transfers)");
            edgeLogger.log("Architecture: PMU devices send data → closest GNB → local collection & grid analysis");
            edgeLogger.log("Edge Devices (PMUs): %d", 
                getDataCentersManager().getComputingNodesGenerator().getMistOnlyList().size());
            edgeLogger.log("Edge Datacenters (GNBs): %d", gnbNodes.size());
            edgeLogger.log("Data Collectors: %d (one per GNB)", gnbDataCollectors.size());
            
            // Log each GNB with its dedicated collector
            for (ComputingNode gnb : gnbNodes) {
                edgeLogger.log("  - %s: Dedicated data collector", gnb.getName());
            }
            
            edgeLogger.flush();
        }
        
        // Also log to simulation console
        simLog.println("PMU DISTRIBUTED GNB Collection Simulation Configuration:");
        simLog.println("  - Collection Mode: PER-GNB DYNAMIC (0.045s window from first arrival)");
        simLog.println("  - Grid analysis: Distributed at each GNB after local collection");
        simLog.println("  - Network Model: PMU → GNB direct transfer");
        simLog.println("  - Total PMUs: " + getDataCentersManager().getComputingNodesGenerator().getMistOnlyList().size());
        simLog.println("  - Total GNBs with dedicated collectors: " + gnbDataCollectors.size());
    }
    
    /**
     * Gets data collector for a specific GNB
     */
    public EdgeDataCollectorDynamic getDataCollectorForGnb(ComputingNode gnb) {
        return gnbDataCollectors.get(gnb);
    }
    
    /**
     * Gets data collector for a PMU (finds closest GNB and returns its collector)
     */
    public EdgeDataCollectorDynamic getDataCollectorForPmu(ComputingNode pmuDevice) {
        ComputingNode closestGnb = findClosestGnbForPmu(pmuDevice);
        if (closestGnb != null) {
            return gnbDataCollectors.get(closestGnb);
        }
        return null;
    }
    
    /**
     * Helper method to find closest GNB for a PMU
     */
    private ComputingNode findClosestGnbForPmu(ComputingNode pmuDevice) {
        if (pmuDevice == null || gnbNodes.isEmpty()) {
            return null;
        }
        
        try {
            ComputingNode closestGnb = null;
            double minDistance = Double.MAX_VALUE;
            
            for (ComputingNode gnb : gnbNodes) {
                double distance = calculateEuclideanDistance(pmuDevice, gnb);
                if (distance < minDistance) {
                    minDistance = distance;
                    closestGnb = gnb;
                }
            }
            
            return closestGnb;
        } catch (Exception e) {
            simLog.println("Error finding closest GNB for PMU: " + e.getMessage());
            return null;
        }
    }
    
    /**
     * Calculate Euclidean distance between two nodes
     */
    private double calculateEuclideanDistance(ComputingNode node1, ComputingNode node2) {
        try {
            com.mechalikh.pureedgesim.locationmanager.Location loc1 = node1.getMobilityModel().getCurrentLocation();
            com.mechalikh.pureedgesim.locationmanager.Location loc2 = node2.getMobilityModel().getCurrentLocation();
            
            double dx = loc1.getXPos() - loc2.getXPos();
            double dy = loc1.getYPos() - loc2.getYPos();
            
            return Math.sqrt(dx * dx + dy * dy);
        } catch (Exception e) {
            return Double.MAX_VALUE;
        }
    }
    
    /**
     * Gets the Edge logger instance
     */
    public EdgeLogger getEdgeLogger() {
        return edgeLogger;
    }
    
    /**
     * Gets data orchestrator status for monitoring
     */
    public String getDataOrchestratorStatus() {
        if (edgeOrchestrator instanceof EdgeTaskOrchestrator) {
            return ((EdgeTaskOrchestrator) edgeOrchestrator).getOrchestratorStatus();
        }
        return "Data orchestrator not active";
    }

    public void processEvent(com.mechalikh.pureedgesim.simulationengine.Event ev) {
        // Handle custom PMU events first
        switch (ev.getTag()) {
            case 300: // EDGE_DATA_RECEIVED
                // Forward to appropriate GNB data collector
                com.mechalikh.pureedgesim.taskgenerator.Task task = 
                    (com.mechalikh.pureedgesim.taskgenerator.Task) ev.getData();
                
                // Find the appropriate data collector for this PMU
                EdgeDataCollectorDynamic collector = getDataCollectorForPmu(task.getEdgeDevice());
                
                if (collector != null) {
                    simLog.println("EdgeSimulationManager - Forwarding PMU data to GNB collector for PMU: " + 
                                 (task.getEdgeDevice() != null ? task.getEdgeDevice().getName() : "unknown"));
                    
                    // Forward to the appropriate GNB data collector
                    collector.processEvent(ev);
                } else {
                    simLog.println("EdgeSimulationManager - No data collector found for PMU: " + 
                                 (task.getEdgeDevice() != null ? task.getEdgeDevice().getName() : "unknown"));
                }
                break;
                
            case 301: // GENERATION_TIME_TIMEOUT
                // This event should be handled by the specific collector that scheduled it
                // We don't need to forward it here as it's already targeted
                super.processEvent(ev);
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
            if (edgeLogger != null) {
                // **Log final statistics for all GNB collectors**
                edgeLogger.log("=== Edge Distributed GNB Collection Simulation Summary ===");
                
                for (Map.Entry<ComputingNode, EdgeDataCollectorDynamic> entry : gnbDataCollectors.entrySet()) {
                    ComputingNode gnb = entry.getKey();
                    EdgeDataCollectorDynamic collector = entry.getValue();
                    edgeLogger.log("%s Collector: %s", gnb.getName(), collector.getStatistics());
                }
                
                edgeLogger.log("Total GNBs with collectors: %d", gnbDataCollectors.size());
                edgeLogger.log("Edge Distributed GNB Collection Simulation completed successfully");
                edgeLogger.close();
            }
            
            simLog.println("Edge Distributed GNB Collection Simulation ended - Check logs in: " + OUTPUT_FOLDER);
            
        } catch (Exception e) {
            simLog.println("Error during Edge simulation cleanup: " + e.getMessage());
            e.printStackTrace();
        }
    }
} 