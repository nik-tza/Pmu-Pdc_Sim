package UpfOnTelco_PdcOnCloud;

import com.mechalikh.pureedgesim.datacentersmanager.ComputingNode;
import com.mechalikh.pureedgesim.network.DefaultNetworkModel;
import com.mechalikh.pureedgesim.network.NetworkLink;
import com.mechalikh.pureedgesim.network.TransferProgress;
import com.mechalikh.pureedgesim.simulationmanager.SimulationManager;
import com.mechalikh.pureedgesim.taskgenerator.Task;
import com.mechalikh.pureedgesim.scenariomanager.SimulationParameters;
import com.mechalikh.pureedgesim.locationmanager.Location;


import java.util.Random;
import java.util.List;

/**
 * PMU Network Model - Data-based transfers
 * PMUs send measurement data instead of tasks to TSO for batch processing
 * Enhanced with realistic jitter, SimulationParameters integration, and distance-based delays
 */
public class CloudNetworkModel extends DefaultNetworkModel {
    
    // Custom transfer type for PMU measurement data
    public static final int SEND_PMU_DATA = 100;
    
    // Jitter for realistic network variation - loaded dynamically from properties
    private static final Random random = new Random();
    private static double CELLULAR_JITTER_MS = 5.0; // Default fallback value
    private static double MAN_JITTER_MS = 2.0; // Default fallback value 
    private static double WAN_JITTER_MS = 8.0; // Default fallback value
    
    // **Distance-based delay parameters - Change this value directly**
    private static final double DISTANCE_DELAY_MICROSECONDS_PER_METER = 4; // μs per meter
    private static final boolean ENABLE_DISTANCE_DELAYS = true; // Set to false for old behavior
    
    // Convert to seconds per meter for internal calculations
    private static final double DISTANCE_DELAY_FACTOR = DISTANCE_DELAY_MICROSECONDS_PER_METER / 1_000_000.0;
    
    // Fixed PMU data size (2KB)
    private static final double PMU_DATA_SIZE_BITS = 2.0 * 8192.0;
    
    // Static block to load jitter values from properties file
    static {
        loadJitterParametersFromProperties();
    }
    
    public CloudNetworkModel(SimulationManager simulationManager) {
        super(simulationManager);
        
        System.out.println("CloudNetworkModel - 3-hop path: PMU → GNB → TELCO → TSO");
        System.out.printf("CloudNetworkModel - Distance delays: %s (%.0fμs/m), Jitter: C=%.1fms M=%.1fms W=%.1fms%n", 
                         ENABLE_DISTANCE_DELAYS ? "ENABLED" : "DISABLED", 
                         DISTANCE_DELAY_MICROSECONDS_PER_METER, CELLULAR_JITTER_MS, MAN_JITTER_MS, WAN_JITTER_MS);
    }
    
    /**
     * Load jitter parameters from simulation_parameters.properties file
     */
    private static void loadJitterParametersFromProperties() {
        try {
            java.util.Properties props = new java.util.Properties();
            String propertiesPath = "UpfOnTelco_PdcOnCloud/settings/simulation_parameters.properties";
            
            // Try to load from file system
            java.io.FileInputStream fis = null;
            try {
                fis = new java.io.FileInputStream(propertiesPath);
                props.load(fis);
                
                // Load jitter values with fallback to defaults
                String cellularJitterStr = props.getProperty("cellular_jitter_ms");
                if (cellularJitterStr != null && !cellularJitterStr.trim().isEmpty()) {
                    CELLULAR_JITTER_MS = Double.parseDouble(cellularJitterStr.trim());
                }
                
                String manJitterStr = props.getProperty("man_jitter_ms");
                if (manJitterStr != null && !manJitterStr.trim().isEmpty()) {
                    MAN_JITTER_MS = Double.parseDouble(manJitterStr.trim());
                }
                
                String wanJitterStr = props.getProperty("wan_jitter_ms");
                if (wanJitterStr != null && !wanJitterStr.trim().isEmpty()) {
                    WAN_JITTER_MS = Double.parseDouble(wanJitterStr.trim());
                }
                
                System.out.printf("CloudNetworkModel - Loaded jitter from properties: C=%.1fms, M=%.1fms, W=%.1fms%n", 
                                CELLULAR_JITTER_MS, MAN_JITTER_MS, WAN_JITTER_MS);
                
            } finally {
                if (fis != null) {
                    fis.close();
                }
            }
            
        } catch (Exception e) {
            System.err.println("CloudNetworkModel - Failed to load jitter parameters from properties: " + e.getMessage());
            System.err.println("CloudNetworkModel - Using default jitter values: C=" + CELLULAR_JITTER_MS + 
                             "ms, M=" + MAN_JITTER_MS + "ms, W=" + WAN_JITTER_MS + "ms");
        }
    }
    
    @Override
    public void send(ComputingNode from, ComputingNode to, Task task, double fileSize, TransferProgress.Type type) {
        // Check if this is PMU measurement data
        if (isPmuDataTransfer(from, to, type)) {
            handlePmuDataTransfer(from, to, task, fileSize, type);
        } else {
            // Use default behavior for other transfers
            super.send(from, to, task, fileSize, type);
        }
    }
    
    @Override
    public void processEvent(com.mechalikh.pureedgesim.simulationengine.Event ev) {
        switch (ev.getTag()) {
        case TRANSFER_FINISHED:
            // Handle PMU data transfer completion
            TransferProgress transfer = (TransferProgress) ev.getData();
            handlePmuDataTransferFinished(transfer);
            break;
        default:
            // Handle other events using parent implementation
            super.processEvent(ev);
            break;
        }
    }
    
    /**
     * Determines if this is a PMU measurement data transfer
     */
    private boolean isPmuDataTransfer(ComputingNode from, ComputingNode to, TransferProgress.Type type) {
        // PMU (mist device) sending to TSO (cloud) with REQUEST type (we repurpose REQUEST for data)
        return from.getType() == com.mechalikh.pureedgesim.scenariomanager.SimulationParameters.TYPES.EDGE_DEVICE 
               && to.getType() == com.mechalikh.pureedgesim.scenariomanager.SimulationParameters.TYPES.CLOUD
               && type == TransferProgress.Type.REQUEST;
    }
    
    /**
     * Handles PMU measurement data transfer with hop time tracking
     */
    private void handlePmuDataTransfer(ComputingNode from, ComputingNode to, Task task, double dataSize, TransferProgress.Type type) {
        // Calculate total network delay
        NetworkTransferResult transferResult = calculateNetworkTransferWithDetails(from, to, task, dataSize);
        
        // Create transfer progress for PMU data
        TransferProgress dataTransfer = new TransferProgress(task, dataSize, type);
        
        // Schedule the TRANSFER_FINISHED event after the calculated network delay
        schedule(this, transferResult.totalDelay, TRANSFER_FINISHED, dataTransfer);
        
        // Notify logger with the transfer results
        if (CloudLogger.getInstance() != null) {
            CloudLogger.getInstance().logPmuNetworkTransfer(task, transferResult);
        }
    }
    
    /**
     * Handles PMU data transfer completion
     */
    private void handlePmuDataTransferFinished(TransferProgress transfer) {
        Task task = transfer.getTask();
        
        // The task has arrived at its destination (TSO)
        // Our custom CloudTaskOrchestrator will then collect the PMU data
        // and create state estimation tasks when sufficient data is available
        scheduleNow(simulationManager, com.mechalikh.pureedgesim.simulationmanager.SimulationManager.SEND_TO_ORCH, task);
    }
    
    /**
     * Network Transfer Result container
     */
    public static class NetworkTransferResult {
        public final String assignedGnbName;
        public final double pmuToGnbTime;
        public final double gnbToTelcoTime;
        public final double telcoToTsoTime;
        public final double totalDelay;
        public final double pmuToGnbDistance;
        public final double gnbToTelcoDistance;
        public final double telcoToTsoDistance;
        
        public NetworkTransferResult(String assignedGnbName, double pmuToGnbTime, double gnbToTelcoTime, 
                                   double telcoToTsoTime, double totalDelay, 
                                   double pmuToGnbDistance, double gnbToTelcoDistance, double telcoToTsoDistance) {
            this.assignedGnbName = assignedGnbName;
            this.pmuToGnbTime = pmuToGnbTime;
            this.gnbToTelcoTime = gnbToTelcoTime;
            this.telcoToTsoTime = telcoToTsoTime;
            this.totalDelay = totalDelay;
            this.pmuToGnbDistance = pmuToGnbDistance;
            this.gnbToTelcoDistance = gnbToTelcoDistance;
            this.telcoToTsoDistance = telcoToTsoDistance;
        }
        
        public String getPathString() {
            return String.format("PMU -> %s (%.4fs, %.1fm) -> TELCO (%.4fs, %.1fm) -> TSO (%.4fs, %.1fm)", 
                               assignedGnbName, pmuToGnbTime, pmuToGnbDistance, 
                               gnbToTelcoTime, gnbToTelcoDistance, telcoToTsoTime, telcoToTsoDistance);
        }
    }
    
    /**
     * Calculates network transfer with full details for use by other components
     */
    public NetworkTransferResult calculateNetworkTransferWithDetails(ComputingNode from, ComputingNode to, Task task, double dataSize) {
        // Get PMU device and find its assigned GNB
        ComputingNode pmuDevice = from;
        ComputingNode assignedGnb = findClosestGnbForPmu(pmuDevice);
        ComputingNode telcoNode = findTelcoNode();
        ComputingNode tsoNode = to; // TSO is the final destination
        
        // Calculate distances
        double pmuToGnbDistance = calculateEuclideanDistance(pmuDevice, assignedGnb);
        double gnbToTelcoDistance = calculateEuclideanDistance(assignedGnb, telcoNode);
        double telcoToTsoDistance = calculateEuclideanDistance(telcoNode, tsoNode);
        
        // Calculate hop times with distance delays
        double pmuToGnbTime = calculateHopTimeWithJitterAndDistance(
            PMU_DATA_SIZE_BITS,
            SimulationParameters.cellularBandwidthBitsPerSecond,
            SimulationParameters.cellularLatency,
            CELLULAR_JITTER_MS / 1000.0,
            pmuDevice, assignedGnb
        );
        
        double gnbToTelcoTime = calculateHopTimeWithJitterAndDistance(
            PMU_DATA_SIZE_BITS,
            SimulationParameters.manBandwidthBitsPerSecond,
            SimulationParameters.manLatency,
            MAN_JITTER_MS / 1000.0,
            assignedGnb, telcoNode
        );
        
        double telcoToTsoTime = calculateHopTimeWithJitterAndDistance(
            PMU_DATA_SIZE_BITS,
            SimulationParameters.wanBandwidthBitsPerSecond,
            SimulationParameters.wanLatency,
            WAN_JITTER_MS / 1000.0,
            telcoNode, tsoNode
        );
        
        // Total network delay
        double totalDelay = pmuToGnbTime + gnbToTelcoTime + telcoToTsoTime;
        
        // Add the delay to the task's actual network time for statistics
        task.addActualNetworkTime(totalDelay);
        
        // Get GNB name
        String gnbName = findGnbForPmu(pmuDevice);
        
        return new NetworkTransferResult(gnbName, pmuToGnbTime, gnbToTelcoTime, telcoToTsoTime, totalDelay,
                                       pmuToGnbDistance, gnbToTelcoDistance, telcoToTsoDistance);
    }
    
    /**
     * **NEW: Calculates hop time with realistic jitter AND distance-based delay**
     */
    private double calculateHopTimeWithJitterAndDistance(double dataSizeInBits, double bandwidthBps, 
                                                        double baseLatency, double jitterSigma,
                                                        ComputingNode fromNode, ComputingNode toNode) {
        // Original calculation
        double transmissionTime = dataSizeInBits / bandwidthBps;
        double jitter = random.nextGaussian() * jitterSigma; // Gaussian jitter
        double baseDelay = transmissionTime + baseLatency + jitter;
        
        // Add distance-based delay if enabled
        if (ENABLE_DISTANCE_DELAYS) {
            double distance = calculateEuclideanDistance(fromNode, toNode);
            double distanceDelay = distance * DISTANCE_DELAY_FACTOR;
            
            return Math.max(0, baseDelay + distanceDelay);
        } else {
            return Math.max(0, baseDelay); // Old behavior - no distance delay
        }
    }
    
    /**
     * **NEW: Calculates Euclidean distance between two nodes**
     */
    private double calculateEuclideanDistance(ComputingNode from, ComputingNode to) {
        Location fromLocation = from.getMobilityModel().getCurrentLocation();
        Location toLocation = to.getMobilityModel().getCurrentLocation();
        
        double dx = fromLocation.getXPos() - toLocation.getXPos();
        double dy = fromLocation.getYPos() - toLocation.getYPos();
        
        return Math.sqrt(dx * dx + dy * dy);
    }
    
    /**
     * **NEW: Finds the closest GNB for a PMU based on distance**
     */
    private ComputingNode findClosestGnbForPmu(ComputingNode pmu) {
        if (pmu == null) return null;
        
        try {
            // Get edge datacenters (GNBs) and filter out TELCO
            List<ComputingNode> allEdgeDatacenters = simulationManager.getDataCentersManager()
                                                                     .getComputingNodesGenerator()
                                                                     .getEdgeOnlyList();
            
            ComputingNode closestGnb = null;
            double minDistance = Double.MAX_VALUE;
            
            for (ComputingNode edge : allEdgeDatacenters) {
                // Skip TELCO node - we want only GNBs
                if (edge.getName() != null && edge.getName().equals("TELCO")) {
                    continue;
                }
                
                double distance = calculateEuclideanDistance(pmu, edge);
                if (distance < minDistance) {
                    minDistance = distance;
                    closestGnb = edge;
                }
            }
            
            return closestGnb;
        } catch (Exception e) {
            System.err.println("CloudNetworkModel - Error finding closest GNB for PMU: " + e.getMessage());
            return null;
        }
    }
    
    /**
     * **NEW: Finds the TELCO node**
     */
    private ComputingNode findTelcoNode() {
        try {
            List<ComputingNode> edgeDatacenters = simulationManager.getDataCentersManager()
                                                                  .getComputingNodesGenerator()
                                                                  .getEdgeOnlyList();
            
            for (ComputingNode edge : edgeDatacenters) {
                if (edge.getName() != null && edge.getName().equals("TELCO")) {
                    return edge;
                }
            }
        } catch (Exception e) {
            System.err.println("CloudNetworkModel - Error finding TELCO node: " + e.getMessage());
        }
        return null;
    }
    
    /**
     * **NEW: Finds the TSO cloud node**
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
            System.err.println("CloudNetworkModel - Error finding TSO node: " + e.getMessage());
        }
        return null;
    }
    
    /**
     * **FIXED: Finds the appropriate GNB for a PMU using distance-based selection**
     */
    private String findGnbForPmu(ComputingNode pmu) {
        if (pmu == null) return "GNB_?";
        
        try {
            // Use distance-based selection
            ComputingNode closestGnb = findClosestGnbForPmu(pmu);
            
            if (closestGnb != null) {
                String edgeName = closestGnb.getName();
                if (edgeName != null) {
                    // Handle different naming conventions
                    if (edgeName.startsWith("EDGE_")) {
                        String edgeId = edgeName.substring(5); // Remove "EDGE_" prefix
                        return "GNB_" + edgeId;
                    } else if (edgeName.startsWith("Edge_")) {
                        String edgeId = edgeName.substring(5); // Remove "Edge_" prefix
                        return "GNB_" + edgeId;
                    } else if (edgeName.toLowerCase().contains("edge")) {
                        // Extract number from edge name using regex
                        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("\\d+");
                        java.util.regex.Matcher matcher = pattern.matcher(edgeName);
                        if (matcher.find()) {
                            String edgeId = matcher.group();
                            return "GNB_" + edgeId;
                        }
                    }
                    
                    // Fallback: Use edge datacenter index if name parsing fails
                    List<ComputingNode> edgeDatacenters = simulationManager.getDataCentersManager()
                                                                          .getComputingNodesGenerator()
                                                                          .getEdgeOnlyList();
                    for (int i = 0; i < edgeDatacenters.size(); i++) {
                        if (edgeDatacenters.get(i).equals(closestGnb)) {
                            return "GNB_" + (i + 1);
                        }
                    }
                }
            }
            
        } catch (Exception e) {
            System.err.println("CloudNetworkModel - Error finding GNB for PMU: " + e.getMessage());
        }
        
        return "GNB_0"; // Default fallback
    }
    
    /**
     * Determines the link type between two nodes
     */
    private NetworkLink.NetworkLinkTypes determineLinkType(ComputingNode from, ComputingNode to) {
        // PMU to TSO typically goes through multiple hops
        if (from.getType() == com.mechalikh.pureedgesim.scenariomanager.SimulationParameters.TYPES.EDGE_DEVICE
            && to.getType() == com.mechalikh.pureedgesim.scenariomanager.SimulationParameters.TYPES.CLOUD) {
            return NetworkLink.NetworkLinkTypes.WAN; // Wide Area Network for PMU to TSO
        }
        
        return NetworkLink.NetworkLinkTypes.LAN; // Default
    }
    
    /**
     * Gets bandwidth between nodes based on their types
     */
    private double getBandwidthBetweenNodes(ComputingNode from, ComputingNode to) {
        // PMU to GNB: Lower bandwidth
        if (from.getType() == com.mechalikh.pureedgesim.scenariomanager.SimulationParameters.TYPES.EDGE_DEVICE) {
            return 10_000_000; // 10 Mbps for PMU uplink
        }
        
        // Default bandwidth
        return 100_000_000; // 100 Mbps
    }
    
    /**
     * Gets latency between nodes based on their types
     */
    private double getLatencyBetweenNodes(ComputingNode from, ComputingNode to) {
        // PMU to TSO: Higher latency due to multiple hops
        if (from.getType() == com.mechalikh.pureedgesim.scenariomanager.SimulationParameters.TYPES.EDGE_DEVICE
            && to.getType() == com.mechalikh.pureedgesim.scenariomanager.SimulationParameters.TYPES.CLOUD) {
            return 0.050; // 50ms for PMU to TSO
        }
        
        // Default latency
        return 0.010; // 10ms
    }
} 