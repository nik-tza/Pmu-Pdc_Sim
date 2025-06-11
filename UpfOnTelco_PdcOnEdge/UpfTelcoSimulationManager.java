package UpfOnTelco_PdcOnEdge;

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
public class UpfTelcoSimulationManager extends DefaultSimulationManager implements OnSimulationStartListener, OnSimulationEndListener {
    
    private UpfTelcoLogger upfTelcoLogger;
    private Map<ComputingNode, UpfTelcoDataCollectorDynamic> gnbDataCollectors; // One collector per GNB
    private List<ComputingNode> gnbNodes; // List of all GNBs
    private static final String OUTPUT_FOLDER = "UpfOnTelco_PdcOnEdge/output/";
    
    public UpfTelcoSimulationManager(SimLog simLog, PureEdgeSim pureEdgeSim, int simulationId, int iteration,
            Scenario scenario) {
        super(simLog, pureEdgeSim, simulationId, iteration, scenario);
        
        try {
            this.upfTelcoLogger = UpfTelcoLogger.initialize(this, OUTPUT_FOLDER);
            simLog.println("PMU Logger initialized successfully");
            
            // Initialize GNB data structures
            this.gnbNodes = new ArrayList<>();
            this.gnbDataCollectors = new HashMap<>();
            
        } catch (Exception e) {
            simLog.println("Failed to initialize PMU Logger: " + e.getMessage());
        }
        
        // Set the custom UpfTelco Network Model for data-based transfers
        setNetworkModel(new UpfTelcoNetworkModel(this));
        
        simLog.println("UpfTelcoSimulationManager initialized with DISTRIBUTED GNB collection");
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
     * **FIXED: Initialize GNB data collectors with exact PMU assignments**
     */
    private void initializeGnbDataCollectors() {
        try {
            gnbDataCollectors = new HashMap<>();
            gnbNodes = new ArrayList<>();
            
            // Get all PMU devices
            List<ComputingNode> pmuDevices = getDataCentersManager()
                    .getComputingNodesGenerator().getMistOnlyList();
            
            // Get all edge datacenters and filter out TELCO to get only GNBs
            List<ComputingNode> edgeDatacenters = getDataCentersManager()
                    .getComputingNodesGenerator().getEdgeOnlyList();
            
            // Filter out TELCO to get only GNBs
            for (ComputingNode edgeDatacenter : edgeDatacenters) {
                if (edgeDatacenter.getName() == null || !edgeDatacenter.getName().equals("TELCO")) {
                    gnbNodes.add(edgeDatacenter);
                }
            }
            
            simLog.println("Found " + gnbNodes.size() + " GNBs and " + pmuDevices.size() + " PMUs for distributed data collection");
            
            // **NEW: Calculate PMU assignment for each GNB based on distance**
            Map<ComputingNode, Integer> gnbPmuCounts = calculatePmuAssignments(gnbNodes, pmuDevices);
            
            // Create one UpfTelcoDataCollectorDynamic per GNB with correct PMU count
            for (ComputingNode gnb : gnbNodes) {
                int expectedPmuCount = gnbPmuCounts.getOrDefault(gnb, 0);
                UpfTelcoDataCollectorDynamic collector = new UpfTelcoDataCollectorDynamic(this, gnb, expectedPmuCount);
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
     * **NEW: Calculate how many PMUs are assigned to each GNB based on closest distance**
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
     * **NEW: Helper method to find closest GNB for a PMU**
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
     * Logs PMU simulation setup information
     */
    private void logPmuSimulationInfo() {
        if (upfTelcoLogger != null) {
            upfTelcoLogger.log("=== UpfTelco Smart Grid DISTRIBUTED GNB Collection Simulation Started ===");
            upfTelcoLogger.log("Collection Mode: PER-GNB DYNAMIC WINDOWS (starts with first data arrival at each GNB after 3-hop path)");
            upfTelcoLogger.log("Collection Window: 0.045 seconds from first PMU data at each GNB (after round trip through TELCO)");
            upfTelcoLogger.log("Network Path: PMU → closest GNB → TELCO → GNB (collection window starts here)");
            upfTelcoLogger.log("Orchestrator: UpfTelcoTaskOrchestrator (Distributed GNB Data Collection)");
            upfTelcoLogger.log("Network Model: UpfTelcoNetworkModel (3-hop path with bidirectional TELCO communication)");
            upfTelcoLogger.log("Architecture: PMU devices send data → closest GNB → TELCO → GNB → local collection & grid analysis");
            upfTelcoLogger.log("UpfTelco Devices (PMUs): %d", 
                getDataCentersManager().getComputingNodesGenerator().getMistOnlyList().size());
            upfTelcoLogger.log("UpfTelco Datacenters (GNBs): %d", gnbNodes.size());
            upfTelcoLogger.log("Data Collectors: %d (one per GNB)", gnbDataCollectors.size());
            
            // Log each GNB with its dedicated collector
            for (ComputingNode gnb : gnbNodes) {
                upfTelcoLogger.log("  - %s: Dedicated data collector (3-hop path processing)", gnb.getName());
            }
            
            upfTelcoLogger.flush();
        }
        
        // Also log to simulation console
        simLog.println("PMU DISTRIBUTED GNB Collection Simulation Configuration:");
        simLog.println("  - Collection Mode: PER-GNB DYNAMIC (0.045s window from first arrival after 3-hop path)");
        simLog.println("  - Network Path: PMU → GNB → TELCO → GNB (collection starts at final GNB)");
        simLog.println("  - Grid analysis: Distributed at each GNB after local collection");
        simLog.println("  - Network Model: 3-hop bidirectional path through TELCO");
        simLog.println("  - Total PMUs: " + getDataCentersManager().getComputingNodesGenerator().getMistOnlyList().size());
        simLog.println("  - Total GNBs with dedicated collectors: " + gnbDataCollectors.size());
    }
    
    /**
     * Gets data collector for a specific GNB
     */
    public UpfTelcoDataCollectorDynamic getDataCollectorForGnb(ComputingNode gnb) {
        return gnbDataCollectors.get(gnb);
    }
    
    /**
     * Gets data collector for a PMU (finds closest GNB and returns its collector)
     */
    public UpfTelcoDataCollectorDynamic getDataCollectorForPmu(ComputingNode pmuDevice) {
        ComputingNode closestGnb = findClosestGnbForPmu(pmuDevice);
        if (closestGnb != null) {
            return gnbDataCollectors.get(closestGnb);
        }
        return null;
    }
    
    /**
     * Returns the UpfTelco Logger instance
     */
    public UpfTelcoLogger getUpfTelcoLogger() {
        return upfTelcoLogger;
    }
    
    /**
     * Get status of data orchestrators
     */
    public String getDataOrchestratorStatus() {
        StringBuilder status = new StringBuilder();
        status.append("GNB Data Collectors Status:\n");
        
        for (Map.Entry<ComputingNode, UpfTelcoDataCollectorDynamic> entry : gnbDataCollectors.entrySet()) {
            String gnbName = entry.getKey().getName();
            UpfTelcoDataCollectorDynamic collector = entry.getValue();
            status.append("  - ").append(gnbName).append(": ").append(collector.getStatistics()).append("\n");
        }
        
        return status.toString();
    }
    
    /**
     * Process events for the simulation manager
     */
    public void processEvent(com.mechalikh.pureedgesim.simulationengine.Event ev) {
        switch (ev.getTag()) {
        // Handle any custom events here if needed
        default:
            super.processEvent(ev);
            break;
        }
    }
    
    @Override
    public void onSimulationEnd() {
        try {
            // Log simulation completion
            if (upfTelcoLogger != null) {
                upfTelcoLogger.log("=== PMU Distributed GNB Collection Simulation Ended ===");
                upfTelcoLogger.log("Final GNB Data Collectors Status:");
                
                for (Map.Entry<ComputingNode, UpfTelcoDataCollectorDynamic> entry : gnbDataCollectors.entrySet()) {
                    String gnbName = entry.getKey().getName();
                    UpfTelcoDataCollectorDynamic collector = entry.getValue();
                    upfTelcoLogger.log("  - %s: %s", gnbName, collector.getStatistics());
                }
                
                upfTelcoLogger.saveAllLogs();
                upfTelcoLogger.close();
            }
            
            simLog.println("PMU Distributed GNB Collection Simulation ended successfully");
            
        } catch (Exception e) {
            simLog.println("ERROR - Failed during PMU simulation end: " + e.getMessage());
            e.printStackTrace();
        }
    }
} 