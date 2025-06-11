package UpfOnEdge_PdcOnEdge;

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
 * Edge Network Model - Data-based transfers
 * Edge devices send measurement data instead of tasks to TSO for batch processing
 * Enhanced with realistic jitter, SimulationParameters integration, and distance-based delays
 */
public class EdgeNetworkModel extends DefaultNetworkModel {
    
    // Custom transfer type for Edge measurement data
    public static final int SEND_EDGE_DATA = 100;
    
    // Jitter for realistic network variation
    private static final Random random = new Random();
    private static final double CELLULAR_JITTER_MS = 5.0; // 5ms std jitter for cellular 
    private static final double MAN_JITTER_MS = 2.0; // 2ms std jitter for MAN 
    private static final double WAN_JITTER_MS = 8.0; // 8ms std jitter for WAN
    
    // **Distance-based delay parameters - Change this value directly**
    private static final double DISTANCE_DELAY_MICROSECONDS_PER_METER = 30; // μs per meter
    private static final boolean ENABLE_DISTANCE_DELAYS = true; // Set to false for old behavior
    
    // Convert to seconds per meter for internal calculations
    private static final double DISTANCE_DELAY_FACTOR = DISTANCE_DELAY_MICROSECONDS_PER_METER / 1_000_000.0;
    
    // Fixed Edge data size (2KB)
    private static final double EDGE_DATA_SIZE_BITS = 2.0 * 8192.0;
    
    public EdgeNetworkModel(SimulationManager simulationManager) {
        super(simulationManager);
        
        System.out.println("EdgeNetworkModel - Initialized with SimulationParameters, realistic jitter, and distance-based delays");
        System.out.printf("EdgeNetworkModel - Distance delays: %s (%.0fμs/m)%n", 
                         ENABLE_DISTANCE_DELAYS ? "ENABLED" : "DISABLED", DISTANCE_DELAY_MICROSECONDS_PER_METER);
    }
    
    @Override
    public void send(ComputingNode from, ComputingNode to, Task task, double fileSize, TransferProgress.Type type) {
        // Check if this is Edge measurement data
        if (isEdgeDataTransfer(from, to, type)) {
            handleEdgeDataTransfer(from, to, task, fileSize, type);
        } else {
            // Use default behavior for other transfers
            super.send(from, to, task, fileSize, type);
        }
    }
    
    @Override
    public void processEvent(com.mechalikh.pureedgesim.simulationengine.Event ev) {
        switch (ev.getTag()) {
        case TRANSFER_FINISHED:
            // Handle Edge data transfer completion
            TransferProgress transfer = (TransferProgress) ev.getData();
            handleEdgeDataTransferFinished(transfer);
            break;
        default:
            // Handle other events using parent implementation
            super.processEvent(ev);
            break;
        }
    }
    
    /**
     * Determines if this is an Edge measurement data transfer
     */
    private boolean isEdgeDataTransfer(ComputingNode from, ComputingNode to, TransferProgress.Type type) {
        // Edge (mist device) sending to GNB (edge datacenter) with REQUEST type (we repurpose REQUEST for data)
        return from.getType() == com.mechalikh.pureedgesim.scenariomanager.SimulationParameters.TYPES.EDGE_DEVICE 
               && to.getType() == com.mechalikh.pureedgesim.scenariomanager.SimulationParameters.TYPES.EDGE_DATACENTER
               && type == TransferProgress.Type.REQUEST;
    }
    
    /**
     * Handles Edge measurement data transfer with hop time tracking (PMU -> GNB only)
     */
    private void handleEdgeDataTransfer(ComputingNode from, ComputingNode to, Task task, double dataSize, TransferProgress.Type type) {
        // Find the closest GNB for this PMU
        ComputingNode targetGnb = findClosestGnbForEdge(from);
        
        if (targetGnb == null) {
            System.err.println("EdgeNetworkModel - No GNB found for PMU: " + from.getName());
            return;
        }
        
        // Calculate PMU -> GNB network delay only
        NetworkTransferResult transferResult = calculateNetworkTransferWithDetails(from, targetGnb, task, dataSize);
        
        // Create transfer progress for Edge data
        TransferProgress dataTransfer = new TransferProgress(task, dataSize, type);
        
        // Schedule the TRANSFER_FINISHED event after the calculated network delay
        schedule(this, transferResult.totalDelay, TRANSFER_FINISHED, dataTransfer);
        
        // Notify logger with the transfer results
        if (EdgeLogger.getInstance() != null) {
            EdgeLogger.getInstance().logEdgeNetworkTransfer(task, transferResult);
        }
    }
    
    /**
     * Handles Edge data transfer completion (PMU data arrived at GNB)
     */
    private void handleEdgeDataTransferFinished(TransferProgress transfer) {
        Task task = transfer.getTask();
        
        // The PMU data has arrived at the GNB
        // Find which GNB received this data and forward to its local data collector
        ComputingNode targetGnb = findClosestGnbForEdge(task.getEdgeDevice());
        
        if (targetGnb != null) {
            // Set the task's destination to the target GNB
            task.setOffloadingDestination(targetGnb);
            
            // Forward to the GNB's orchestrator for local collection and processing
        scheduleNow(simulationManager, com.mechalikh.pureedgesim.simulationmanager.SimulationManager.SEND_TO_ORCH, task);
            
            System.out.printf("EdgeNetworkModel - PMU data from %s delivered to GNB %s%n", 
                             task.getEdgeDevice().getName(), targetGnb.getName());
        } else {
            System.err.println("EdgeNetworkModel - Failed to find target GNB for PMU data delivery");
        }
    }
    
    /**
     * Network Transfer Result container (PMU -> GNB only)
     */
    public static class NetworkTransferResult {
        public final String assignedGnbName;
        public final double edgeToGnbTime;
        public final double totalDelay;
        public final double edgeToGnbDistance;
        
        public NetworkTransferResult(String assignedGnbName, double edgeToGnbTime, double totalDelay, double edgeToGnbDistance) {
            this.assignedGnbName = assignedGnbName;
            this.edgeToGnbTime = edgeToGnbTime;
            this.totalDelay = totalDelay;
            this.edgeToGnbDistance = edgeToGnbDistance;
        }
        
        public String getPathString() {
            return String.format("PMU -> %s (%.4fs, %.1fm)", 
                               assignedGnbName, edgeToGnbTime, edgeToGnbDistance);
        }
    }
    
    /**
     * Calculates network transfer with PMU -> GNB hop only
     */
    public NetworkTransferResult calculateNetworkTransferWithDetails(ComputingNode from, ComputingNode to, Task task, double dataSize) {
        // Get Edge device 
        ComputingNode edgeDevice = from;
        
        // **FIXED: Find the actual closest GNB and use it for both distance and naming**
        ComputingNode actualClosestGnb = findClosestGnbForEdge(edgeDevice);
        if (actualClosestGnb == null) {
            actualClosestGnb = to; // Fallback to passed GNB
        }
        
        // Calculate distance to the actual closest GNB
        double edgeToGnbDistance = calculateEuclideanDistance(edgeDevice, actualClosestGnb);
        
        // Calculate hop time with distance delays (PMU -> actual closest GNB)
        double edgeToGnbTime = calculateHopTimeWithJitterAndDistance(
            EDGE_DATA_SIZE_BITS,
            SimulationParameters.cellularBandwidthBitsPerSecond,
            SimulationParameters.cellularLatency,
            CELLULAR_JITTER_MS / 1000.0,
            edgeDevice, actualClosestGnb
        );
        
        // Total network delay is just PMU -> GNB
        double totalDelay = edgeToGnbTime;
        
        // Add the delay to the task's actual network time for statistics
        task.addActualNetworkTime(totalDelay);
        
        // Get GNB name using the same GNB node
        String gnbName = getGnbNameFromNode(actualClosestGnb);
        
        return new NetworkTransferResult(gnbName, edgeToGnbTime, totalDelay, edgeToGnbDistance);
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
     * **NEW: Finds the closest GNB for an Edge device based on distance**
     */
    private ComputingNode findClosestGnbForEdge(ComputingNode edge) {
        if (edge == null) return null;
        
        try {
            // Get edge datacenters (GNBs) and filter out TELCO
            List<ComputingNode> allEdgeDatacenters = simulationManager.getDataCentersManager()
                                                                     .getComputingNodesGenerator()
                                                                     .getEdgeOnlyList();
            
            ComputingNode closestGnb = null;
            double minDistance = Double.MAX_VALUE;
            
            for (ComputingNode edgeDatacenter : allEdgeDatacenters) {
                // Skip TELCO node - we want only GNBs
                if (edgeDatacenter.getName() != null && edgeDatacenter.getName().equals("TELCO")) {
                    continue;
                }
                
                double distance = calculateEuclideanDistance(edge, edgeDatacenter);
                if (distance < minDistance) {
                    minDistance = distance;
                    closestGnb = edgeDatacenter;
                }
            }
            
            return closestGnb;
        } catch (Exception e) {
            System.err.println("EdgeNetworkModel - Error finding closest GNB for Edge device: " + e.getMessage());
            return null;
        }
    }
    
    // REMOVED: findTelcoNode() and findTsoNode() - not needed for PMU -> GNB only architecture
    
    /**
     * **NEW: Get GNB name from a specific GNB node**
     */
    private String getGnbNameFromNode(ComputingNode gnbNode) {
        if (gnbNode == null) return "GNB_?";
        
        try {
            String edgeName = gnbNode.getName();
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
                    if (edgeDatacenters.get(i).equals(gnbNode)) {
                        return "GNB_" + (i + 1);
                    }
                }
            }
            
        } catch (Exception e) {
            System.err.println("EdgeNetworkModel - Error getting GNB name from node: " + e.getMessage());
        }
        
        return "GNB_0"; // Default fallback
    }

    /**
     * **FIXED: Finds the appropriate GNB for an Edge device using distance-based selection**
     */
    private String findGnbForEdge(ComputingNode edge) {
        if (edge == null) return "GNB_?";
        
        try {
            // Use distance-based selection
            ComputingNode closestGnb = findClosestGnbForEdge(edge);
            return getGnbNameFromNode(closestGnb);
            
        } catch (Exception e) {
            System.err.println("EdgeNetworkModel - Error finding GNB for Edge device: " + e.getMessage());
        }
        
        return "GNB_0"; // Default fallback
    }
    
    /**
     * Determines the link type between two nodes
     */
    private NetworkLink.NetworkLinkTypes determineLinkType(ComputingNode from, ComputingNode to) {
        // Edge to TSO typically goes through multiple hops
        if (from.getType() == com.mechalikh.pureedgesim.scenariomanager.SimulationParameters.TYPES.EDGE_DEVICE
            && to.getType() == com.mechalikh.pureedgesim.scenariomanager.SimulationParameters.TYPES.CLOUD) {
            return NetworkLink.NetworkLinkTypes.WAN; // Wide Area Network for Edge to TSO
        }
        
        return NetworkLink.NetworkLinkTypes.LAN; // Default
    }
    
    /**
     * Gets bandwidth between nodes based on their types
     */
    private double getBandwidthBetweenNodes(ComputingNode from, ComputingNode to) {
        // Edge to GNB: Lower bandwidth
        if (from.getType() == com.mechalikh.pureedgesim.scenariomanager.SimulationParameters.TYPES.EDGE_DEVICE) {
            return 10_000_000; // 10 Mbps for Edge uplink
        }
        
        // Default bandwidth
        return 100_000_000; // 100 Mbps
    }
    
    /**
     * Gets latency between nodes based on their types
     */
    private double getLatencyBetweenNodes(ComputingNode from, ComputingNode to) {
        // Edge to TSO: Higher latency due to multiple hops
        if (from.getType() == com.mechalikh.pureedgesim.scenariomanager.SimulationParameters.TYPES.EDGE_DEVICE
            && to.getType() == com.mechalikh.pureedgesim.scenariomanager.SimulationParameters.TYPES.CLOUD) {
            return 0.050; // 50ms for Edge to TSO
        }
        
        // Default latency
        return 0.010; // 10ms
    }
} 