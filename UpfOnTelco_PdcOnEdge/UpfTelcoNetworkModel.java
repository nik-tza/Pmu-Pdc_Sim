package UpfOnTelco_PdcOnEdge;

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
 * UpfTelco Network Model - 3-hop Data Transfer (PMU → GNB → TELCO → GNB)
 * Based on scenarios 1 and 3 with simple random jitter for PMU variation
 */
public class UpfTelcoNetworkModel extends DefaultNetworkModel {
    
    // Custom transfer type for UpfTelco measurement data
    public static final int SEND_UPFTELCO_DATA = 100;
    
    // Jitter for realistic network variation
    private static final Random random = new Random();
    private static final double CELLULAR_JITTER_MS = 5.0; // 5ms std jitter for cellular 
    private static final double MAN_JITTER_MS = 2.0; // 2ms std jitter for MAN 
    
    // Distance-based delay parameters
    private static final double DISTANCE_DELAY_MICROSECONDS_PER_METER = 5; // Reduced from 30 for realism
    private static final boolean ENABLE_DISTANCE_DELAYS = true; 
    private static final double DISTANCE_DELAY_FACTOR = DISTANCE_DELAY_MICROSECONDS_PER_METER / 1_000_000.0;
    
    // PMU jitter enhancement for variation
    private static final double PMU_JITTER_MULTIPLIER = 1.5; // Extra randomness for PMUs
    
    // Fixed UpfTelco data size (2KB)
    private static final double UPFTELCO_DATA_SIZE_BITS = 2.0 * 8192.0;
    
    public UpfTelcoNetworkModel(SimulationManager simulationManager) {
        super(simulationManager);
        System.out.println("UpfTelcoNetworkModel - 3-hop path: PMU → GNB → TELCO → GNB");
        System.out.printf("UpfTelcoNetworkModel - Distance delays: %s (%.0fμs/m), PMU jitter x%.1f%n", 
                         ENABLE_DISTANCE_DELAYS ? "ENABLED" : "DISABLED", 
                         DISTANCE_DELAY_MICROSECONDS_PER_METER, PMU_JITTER_MULTIPLIER);
    }
    
    @Override
    public void send(ComputingNode from, ComputingNode to, Task task, double fileSize, TransferProgress.Type type) {
        if (isUpfTelcoDataTransfer(from, to, type)) {
            handleUpfTelcoDataTransfer(from, to, task, fileSize, type);
        } else {
            super.send(from, to, task, fileSize, type);
        }
    }
    
    @Override
    public void processEvent(com.mechalikh.pureedgesim.simulationengine.Event ev) {
        switch (ev.getTag()) {
        case TRANSFER_FINISHED:
            TransferProgress transfer = (TransferProgress) ev.getData();
            handleUpfTelcoDataTransferFinished(transfer);
            break;
        default:
            super.processEvent(ev);
            break;
        }
    }
    
    private boolean isUpfTelcoDataTransfer(ComputingNode from, ComputingNode to, TransferProgress.Type type) {
        return from.getType() == com.mechalikh.pureedgesim.scenariomanager.SimulationParameters.TYPES.EDGE_DEVICE 
               && to.getType() == com.mechalikh.pureedgesim.scenariomanager.SimulationParameters.TYPES.EDGE_DATACENTER
               && type == TransferProgress.Type.REQUEST;
    }
    
    private void handleUpfTelcoDataTransfer(ComputingNode from, ComputingNode to, Task task, double dataSize, TransferProgress.Type type) {
        ComputingNode targetGnb = findClosestGnbForUpfTelco(from);
        
        if (targetGnb == null) {
            System.err.println("UpfTelcoNetworkModel - No GNB found for PMU: " + from.getName());
            return;
        }
        
        // Calculate 3-hop network delay
        NetworkTransferResult transferResult = calculateNetworkTransferWithDetails(from, targetGnb, task, dataSize);
        
        TransferProgress dataTransfer = new TransferProgress(task, dataSize, type);
        schedule(this, transferResult.totalDelay, TRANSFER_FINISHED, dataTransfer);
        
        // Log the transfer
        if (UpfTelcoLogger.getInstance() != null) {
            UpfTelcoLogger.getInstance().logUpfTelcoNetworkTransfer(task, transferResult);
        }
    }
    
    private void handleUpfTelcoDataTransferFinished(TransferProgress transfer) {
        Task task = transfer.getTask();
        ComputingNode targetGnb = findClosestGnbForUpfTelco(task.getEdgeDevice());
        
        if (targetGnb != null) {
            task.setOffloadingDestination(targetGnb);
            scheduleNow(simulationManager, com.mechalikh.pureedgesim.simulationmanager.SimulationManager.SEND_TO_ORCH, task);
        }
    }
    
    /**
     * Network Transfer Result for 3-hop path
     */
    public static class NetworkTransferResult {
        public final String assignedGnbName;
        public final double upfTelcoToGnbTime;
        public final double gnbToTelcoTime;
        public final double telcoToGnbTime;
        public final double totalDelay;
        public final double upfTelcoToGnbDistance;
        public final double gnbToTelcoDistance;
        public final double telcoToGnbDistance;
        
        public NetworkTransferResult(String assignedGnbName, double upfTelcoToGnbTime, double gnbToTelcoTime, 
                                   double telcoToGnbTime, double totalDelay, 
                                   double upfTelcoToGnbDistance, double gnbToTelcoDistance, double telcoToGnbDistance) {
            this.assignedGnbName = assignedGnbName;
            this.upfTelcoToGnbTime = upfTelcoToGnbTime;
            this.gnbToTelcoTime = gnbToTelcoTime;
            this.telcoToGnbTime = telcoToGnbTime;
            this.totalDelay = totalDelay;
            this.upfTelcoToGnbDistance = upfTelcoToGnbDistance;
            this.gnbToTelcoDistance = gnbToTelcoDistance;
            this.telcoToGnbDistance = telcoToGnbDistance;
        }
        
        public String getPathString() {
            return String.format("PMU -> %s (%.4fs, %.1fm) -> TELCO (%.4fs, %.1fm) -> %s (%.4fs, %.1fm)", 
                               assignedGnbName, upfTelcoToGnbTime, upfTelcoToGnbDistance,
                               gnbToTelcoTime, gnbToTelcoDistance,
                               assignedGnbName, telcoToGnbTime, telcoToGnbDistance);
        }
    }
    
    /**
     * Calculates 3-hop network transfer: PMU → GNB → TELCO → GNB
     */
    public NetworkTransferResult calculateNetworkTransferWithDetails(ComputingNode from, ComputingNode to, Task task, double dataSize) {
        ComputingNode upfTelcoDevice = from;
        ComputingNode assignedGnb = findClosestGnbForUpfTelco(upfTelcoDevice);
        ComputingNode telcoNode = findTelcoNode();
        
        if (assignedGnb == null) {
            assignedGnb = to; // Fallback
        }
        if (telcoNode == null) {
            System.err.println("UpfTelcoNetworkModel - ERROR: TELCO node not found!");
            // Fallback to 1-hop
            double fallbackTime = calculateHopTimeWithJitterAndDistance(
                UPFTELCO_DATA_SIZE_BITS,
                SimulationParameters.cellularBandwidthBitsPerSecond,
                SimulationParameters.cellularLatency,
                CELLULAR_JITTER_MS / 1000.0,
                upfTelcoDevice, assignedGnb, true // isPmuToGnb = true
            );
            double fallbackDistance = calculateEuclideanDistance(upfTelcoDevice, assignedGnb);
            String gnbName = getGnbNameFromNode(assignedGnb);
            return new NetworkTransferResult(gnbName, fallbackTime, 0, 0, fallbackTime, fallbackDistance, 0, 0);
        }
        
        // Calculate distances for all 3 hops
        double upfTelcoToGnbDistance = calculateEuclideanDistance(upfTelcoDevice, assignedGnb);
        double gnbToTelcoDistance = calculateEuclideanDistance(assignedGnb, telcoNode);
        double telcoToGnbDistance = calculateEuclideanDistance(telcoNode, assignedGnb);
        
        // Calculate hop times with different jitter characteristics
        // Hop 1: PMU → GNB (cellular with enhanced PMU jitter)
        double upfTelcoToGnbTime = calculateHopTimeWithJitterAndDistance(
            UPFTELCO_DATA_SIZE_BITS,
            SimulationParameters.cellularBandwidthBitsPerSecond,
            SimulationParameters.cellularLatency,
            CELLULAR_JITTER_MS / 1000.0,
            upfTelcoDevice, assignedGnb, true // isPmuToGnb = true (enhanced jitter)
        );
        
        // Hop 2: GNB → TELCO (MAN)
        double gnbToTelcoTime = calculateHopTimeWithJitterAndDistance(
            UPFTELCO_DATA_SIZE_BITS,
            SimulationParameters.manBandwidthBitsPerSecond,
            SimulationParameters.manLatency,
            MAN_JITTER_MS / 1000.0,
            assignedGnb, telcoNode, false // isPmuToGnb = false (normal jitter)
        );
        
        // Hop 3: TELCO → GNB (MAN return)
        double telcoToGnbTime = calculateHopTimeWithJitterAndDistance(
            UPFTELCO_DATA_SIZE_BITS,
            SimulationParameters.manBandwidthBitsPerSecond,
            SimulationParameters.manLatency,
            MAN_JITTER_MS / 1000.0,
            telcoNode, assignedGnb, false // isPmuToGnb = false (normal jitter)
        );
        
        double totalDelay = upfTelcoToGnbTime + gnbToTelcoTime + telcoToGnbTime;
        task.addActualNetworkTime(totalDelay);
        
        String gnbName = getGnbNameFromNode(assignedGnb);
        return new NetworkTransferResult(gnbName, upfTelcoToGnbTime, gnbToTelcoTime, telcoToGnbTime, totalDelay,
                                       upfTelcoToGnbDistance, gnbToTelcoDistance, telcoToGnbDistance);
    }
    
    /**
     * Calculates hop time with jitter and distance - BASED ON SCENARIOS 1 & 3
     */
    private double calculateHopTimeWithJitterAndDistance(double dataSizeInBits, double bandwidthBps, 
                                                        double baseLatency, double jitterSigma,
                                                        ComputingNode fromNode, ComputingNode toNode,
                                                        boolean isPmuToGnb) {
        // Basic transmission time
        double transmissionTime = dataSizeInBits / bandwidthBps;
        
        // Apply jitter - enhanced for PMU variations like scenarios 1 & 3
        double jitter;
        if (isPmuToGnb) {
            // Enhanced PMU jitter for network variation (like other scenarios)
            jitter = random.nextGaussian() * jitterSigma * PMU_JITTER_MULTIPLIER;
            
            // Add extra PMU-specific randomness based on PMU ID 
            Random pmuRandom = new Random(fromNode.getId());
            jitter += pmuRandom.nextGaussian() * jitterSigma * 0.5; // Additional variation
        } else {
            // Normal jitter for GNB-TELCO communication
            jitter = random.nextGaussian() * jitterSigma;
        }
        
        double baseDelay = transmissionTime + baseLatency + jitter;
        
        // Add distance-based delay if enabled
        if (ENABLE_DISTANCE_DELAYS) {
            double distance = calculateEuclideanDistance(fromNode, toNode);
            double distanceDelay = distance * DISTANCE_DELAY_FACTOR;
            return Math.max(0, baseDelay + distanceDelay);
        } else {
            return Math.max(0, baseDelay);
        }
    }
    
    /**
     * Calculates Euclidean distance between two nodes
     */
    private double calculateEuclideanDistance(ComputingNode from, ComputingNode to) {
        Location fromLocation = from.getMobilityModel().getCurrentLocation();
        Location toLocation = to.getMobilityModel().getCurrentLocation();
        
        double dx = fromLocation.getXPos() - toLocation.getXPos();
        double dy = fromLocation.getYPos() - toLocation.getYPos();
        
        return Math.sqrt(dx * dx + dy * dy);
    }
    
    /**
     * Finds the closest GNB for an UpfTelco device
     */
    private ComputingNode findClosestGnbForUpfTelco(ComputingNode upfTelco) {
        if (upfTelco == null) return null;
        
        try {
            List<ComputingNode> allEdgeDatacenters = simulationManager.getDataCentersManager()
                                                                     .getComputingNodesGenerator()
                                                                     .getEdgeOnlyList();
            
            ComputingNode closestGnb = null;
            double minDistance = Double.MAX_VALUE;
            
            for (ComputingNode edgeDatacenter : allEdgeDatacenters) {
                // Skip TELCO node
                if (edgeDatacenter.getName() != null && edgeDatacenter.getName().equals("TELCO")) {
                    continue;
                }
                
                double distance = calculateEuclideanDistance(upfTelco, edgeDatacenter);
                if (distance < minDistance) {
                    minDistance = distance;
                    closestGnb = edgeDatacenter;
                }
            }
            
            return closestGnb;
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Finds the TELCO node
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
            System.err.println("UpfTelcoNetworkModel - Error finding TELCO node: " + e.getMessage());
        }
        return null;
    }
    
    /**
     * Gets GNB name from ComputingNode
     */
    private String getGnbNameFromNode(ComputingNode gnbNode) {
        if (gnbNode == null) {
            return "Unknown";
        }
        
        String gnbName = gnbNode.getName();
        if (gnbName != null && gnbName.startsWith("EDGE_")) {
            try {
                String gnbId = gnbName.substring(5);
                return "GNB_" + gnbId;
            } catch (Exception e) {
                System.err.println("UpfTelcoNetworkModel - Could not extract GNB ID from: " + gnbName);
            }
        }
        
        return "GNB_Unknown";
    }
} 