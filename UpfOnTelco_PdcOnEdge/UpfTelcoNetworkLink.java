package UpfOnTelco_PdcOnEdge;

import com.mechalikh.pureedgesim.datacentersmanager.ComputingNode;
import com.mechalikh.pureedgesim.network.NetworkLink;
import com.mechalikh.pureedgesim.network.NetworkModel;
import com.mechalikh.pureedgesim.network.TransferProgress;
import com.mechalikh.pureedgesim.scenariomanager.SimulationParameters;
import com.mechalikh.pureedgesim.simulationmanager.SimulationManager;

/**
 * PMU Network Link - Handles measurement data transfers
 * Calculates hop-based delays for PMU data transmission to GNB (distributed architecture)
 */
public class UpfTelcoNetworkLink extends NetworkLink {
    
    public UpfTelcoNetworkLink(ComputingNode src, ComputingNode dest, SimulationManager simulationManager, NetworkLinkTypes type) {
        super(src, dest, simulationManager, type);
        System.out.printf("PmuNetworkLink - Created link: %s → %s (type: %s)%n", 
                         src.getName(), dest.getName(), type);
    }

    @Override
    protected void transferFinished(TransferProgress transfer) {
        // Remove from transfer list
        this.transferProgressList.remove(transfer);

        // Calculate hop delays for PMU data
        calculatePmuDataHopDelays(transfer);

        // Update logger parameters
        simulationManager.getSimulationLogger().updateNetworkUsage(transfer);

        // Handle based on transfer type
        if (transfer.getTransferType() == TransferProgress.Type.REQUEST) {
            // PMU measurement data received - trigger data collection at appropriate GNB
                    if (simulationManager instanceof UpfTelcoSimulationManager) {
            UpfTelcoSimulationManager upfTelcoManager = (UpfTelcoSimulationManager) simulationManager;
                
                // Find the appropriate GNB collector for this PMU data
                UpfTelcoDataCollectorDynamic pmuCollector = upfTelcoManager.getDataCollectorForPmu(transfer.getTask().getEdgeDevice());
                if (pmuCollector != null) {
                    // Forward to the correct GNB collector
                    scheduleNow(pmuCollector, UpfTelcoDataCollectorDynamic.UPFTELCO_DATA_RECEIVED, transfer.getTask());
                    System.out.printf("PmuNetworkLink - Forwarded PMU data to GNB collector: Task %d%n", transfer.getTask().getId());
                } else {
                    System.err.printf("PmuNetworkLink - ERROR: No GNB collector found for PMU task %d%n", transfer.getTask().getId());
                }
            }
        } else {
            // For other transfers, use default behavior
            scheduleNow(simulationManager.getNetworkModel(), 2, transfer); // TRANSFER_FINISHED = 2
        }
    }
    
    /**
     * Calculates hop delays for PMU measurement data transmission
     */
    private void calculatePmuDataHopDelays(TransferProgress transfer) {
        // PMU data transmission path (NEW): PMU → GNB (local processing)
        
        // Get source and destination names
        String sourceName = getSrc().getName();
        String destName = getDst().getName();
        
        // Extract PMU ID from source name
        int pmuId = extractPmuId(sourceName);
        
        // Calculate hop delays based on PMU network topology and data size
        double fileSize = transfer.getFileSize();
        
        // Calculate delay for PMU → GNB hop (NEW: only 1 hop needed)
        double pmuToGnbDelay = calculateHopDelay(1, pmuId, fileSize); // PMU → GNB (wireless)
        
        // Add hop delay to task (legacy: keep hop number 1 for compatibility)
        transfer.getTask().addHopDelay(1, pmuToGnbDelay);
        
        // Total network time for the data transfer (NEW: only PMU → GNB)
        double totalDelay = pmuToGnbDelay;
        transfer.getTask().addActualNetworkTime(totalDelay);
        
        System.out.printf("PmuNetworkLink - Data transfer delays: PMU_%d → GNB: %.4fs (Total: %.4fs)%n",
                         pmuId, pmuToGnbDelay, totalDelay);
    }
    
    /**
     * Handles when PMU measurement data is received by GNB (NEW: distributed architecture)
     */
    private void handlePmuDataReceived(TransferProgress transfer) {
        System.out.printf("PmuNetworkLink - PMU measurement data received by GNB from %s at time %.4f%n",
                         getSrc().getName(), simulationManager.getSimulation().clock());
        
        // Forward the task to appropriate GNB data collector for batch processing
        if (simulationManager instanceof UpfTelcoSimulationManager) {
            UpfTelcoSimulationManager upfTelcoSimManager = (UpfTelcoSimulationManager) simulationManager;
            
            // Find the appropriate GNB collector for this PMU data
            UpfTelcoDataCollectorDynamic pmuCollector = upfTelcoSimManager.getDataCollectorForPmu(transfer.getTask().getEdgeDevice());
            if (pmuCollector != null) {
                // Forward to the correct GNB collector
                scheduleNow(pmuCollector, UpfTelcoDataCollectorDynamic.UPFTELCO_DATA_RECEIVED, transfer.getTask());
            } else {
                // Fallback: schedule data collection event directly to simulation manager
                scheduleNow(simulationManager, 300, transfer.getTask()); // UPFTELCO_DATA_RECEIVED = 300
            }
        } else {
            // Fallback: schedule data collection event directly
            scheduleNow(simulationManager, 300, transfer.getTask()); // UPFTELCO_DATA_RECEIVED = 300
        }
    }
    
    /**
     * Calculates delay for a specific hop based on PMU ID, hop number and data size
     */
    private double calculateHopDelay(int hopNumber, int pmuId, double fileSize) {
        // Get bandwidth and latency based on hop type
        double bandwidth = getHopBandwidth(hopNumber);
        double latency = getHopLatency(hopNumber);
        
        // Calculate transfer delay based on data size and bandwidth
        double transferDelay = fileSize / bandwidth;
        
        // Add fixed latency for this hop type
        double totalDelay = transferDelay + latency;
        
        // Add small random variation (±10%) to simulate network conditions
        double variation = (Math.random() - 0.5) * 0.2 * totalDelay;
        return Math.max(0.001, totalDelay + variation); // Minimum 1ms
    }
    
    /**
     * Returns the bandwidth for a specific hop type from simulation parameters
     */
    private double getHopBandwidth(int hopNumber) {
        switch (hopNumber) {
            case 1: // PMU → GNB (wireless/cellular)
                return SimulationParameters.cellularBandwidthBitsPerSecond;
            case 2: // GNB → TELCO (fiber/MAN)
                return SimulationParameters.manBandwidthBitsPerSecond;
            case 3: // TELCO → TSO (WAN)
                return SimulationParameters.wanBandwidthBitsPerSecond;
            default:
                return SimulationParameters.manBandwidthBitsPerSecond;
        }
    }
    
    /**
     * Returns the latency for a specific hop type from simulation parameters
     */
    private double getHopLatency(int hopNumber) {
        switch (hopNumber) {
            case 1: // PMU → GNB (wireless/cellular)
                return SimulationParameters.cellularLatency;
            case 2: // GNB → TELCO (fiber/MAN)
                return SimulationParameters.manLatency;
            case 3: // TELCO → TSO (WAN)
                return SimulationParameters.wanLatency;
            default:
                return SimulationParameters.manLatency;
        }
    }
    
    /**
     * Extracts PMU ID from device name
     */
    private int extractPmuId(String deviceName) {
        if (deviceName != null && deviceName.startsWith("PMU_")) {
            try {
                return Integer.parseInt(deviceName.substring(4));
            } catch (NumberFormatException e) {
                // Fallback
            }
        }
        return 0; // Default PMU ID
    }
} 