package UpfOnTelco_PdcOnCloud;

import com.mechalikh.pureedgesim.datacentersmanager.ComputingNode;
import com.mechalikh.pureedgesim.network.NetworkLink;
import com.mechalikh.pureedgesim.network.NetworkModel;
import com.mechalikh.pureedgesim.network.TransferProgress;
import com.mechalikh.pureedgesim.scenariomanager.SimulationParameters;
import com.mechalikh.pureedgesim.simulationmanager.SimulationManager;

/**
 * PMU Network Link - Handles measurement data transfers
 * Calculates hop-based delays for PMU data transmission to TSO
 */
public class CloudNetworkLink extends NetworkLink {
    
    public CloudNetworkLink(ComputingNode src, ComputingNode dest, SimulationManager simulationManager, NetworkLinkTypes type) {
        super(src, dest, simulationManager, type);
        System.out.printf("CloudNetworkLink - Created link: %s → %s (type: %s)%n", 
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
            // PMU measurement data received by TSO - trigger data collection
            if (simulationManager instanceof CloudSimulationManager) {
                CloudSimulationManager cloudManager = (CloudSimulationManager) simulationManager;
                if (cloudManager.getDataCollector() != null) {
                    // Forward to data collector
                    scheduleNow(cloudManager.getDataCollector(), CloudDataCollectorDynamic.PMU_DATA_RECEIVED, transfer.getTask());
                    System.out.printf("CloudNetworkLink - Forwarded PMU data to collector: Task %d%n", transfer.getTask().getId());
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
        // PMU data transmission path: PMU → GNB → TELCO → TSO
        
        // Get source and destination names
        String sourceName = getSrc().getName();
        String destName = getDst().getName();
        
        // Extract PMU ID from source name
        int pmuId = extractPmuId(sourceName);
        
        // Calculate hop delays based on PMU network topology and data size
        double fileSize = transfer.getFileSize();
        
        // Calculate delays for each hop
        double hop1Delay = calculateHopDelay(1, pmuId, fileSize); // PMU → GNB (wireless)
        double hop2Delay = calculateHopDelay(2, pmuId, fileSize); // GNB → TELCO (fiber)
        double hop3Delay = calculateHopDelay(3, pmuId, fileSize); // TELCO → TSO (dedicated)
        
        // Add hop delays to task
        transfer.getTask().addHopDelay(1, hop1Delay);
        transfer.getTask().addHopDelay(2, hop2Delay);
        transfer.getTask().addHopDelay(3, hop3Delay);
        
        // Total network time for the data transfer
        double totalDelay = hop1Delay + hop2Delay + hop3Delay;
        transfer.getTask().addActualNetworkTime(totalDelay);
        
        System.out.printf("CloudNetworkLink - Data transfer delays: PMU_%d → Hop1: %.4fs, Hop2: %.4fs, Hop3: %.4fs (Total: %.4fs)%n",
                         pmuId, hop1Delay, hop2Delay, hop3Delay, totalDelay);
    }
    
    /**
     * Handles when PMU measurement data is received by TSO
     */
    private void handlePmuDataReceived(TransferProgress transfer) {
        System.out.printf("CloudNetworkLink - PMU measurement data received by TSO from %s at time %.4f%n",
                         getSrc().getName(), simulationManager.getSimulation().clock());
        
        // Forward the task to data collector for batch processing
        // Instead of normal task execution, trigger data collection
        if (simulationManager instanceof CloudSimulationManager) {
            CloudSimulationManager cloudSimManager = (CloudSimulationManager) simulationManager;
            
            // Schedule the PMU data received event directly to the simulation manager
            scheduleNow(cloudSimManager, 300, transfer.getTask()); // PMU_DATA_RECEIVED = 300
        } else {
            // Fallback: schedule data collection event directly
            scheduleNow(simulationManager, 300, transfer.getTask()); // PMU_DATA_RECEIVED = 300
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