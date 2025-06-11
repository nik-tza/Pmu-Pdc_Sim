package UpfOnEdge_PdcOnEdge;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Random;
import org.apache.commons.math3.distribution.NormalDistribution;

import com.mechalikh.pureedgesim.datacentersmanager.ComputingNode;
import com.mechalikh.pureedgesim.scenariomanager.SimulationParameters;
import com.mechalikh.pureedgesim.simulationengine.FutureQueue;
import com.mechalikh.pureedgesim.simulationmanager.SimulationManager;
import com.mechalikh.pureedgesim.taskgenerator.DefaultTaskGenerator;
import com.mechalikh.pureedgesim.taskgenerator.Task;

public class EdgeTaskGenerator extends DefaultTaskGenerator {
    protected Random random;
    private final int TASKS_PER_SECOND; // **NEW: Read from applications.xml**
    private final NormalDistribution requestSizeDistribution;
    
    // Parameters for normal distribution of request size (PMU measurement data)
    private static final double MEAN_REQUEST_SIZE = 5; // KB
    private static final double REQUEST_SIZE_STD_DEV = 1; // KB
    
    // PMU data transfers are pure data transmissions, not computational tasks
    private static final long PMU_DATA_LENGTH = 10; // Minimal computation for proper task flow
    
    public EdgeTaskGenerator(SimulationManager simulationManager) {
        super(simulationManager);
        try {
            random = SecureRandom.getInstanceStrong();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        
        // **NEW: Read PMU generation rate from applications.xml**
        this.TASKS_PER_SECOND = readPmuRateFromApplications();
        
        // Initialize normal distribution for data size only
        requestSizeDistribution = new NormalDistribution(MEAN_REQUEST_SIZE, REQUEST_SIZE_STD_DEV);
        
        System.out.println("PmuTaskGenerator - Initialized for PMU data transfers:");
        System.out.println("  - Data frequency: " + TASKS_PER_SECOND + " measurements/second per PMU");
        System.out.println("  - Data size: " + MEAN_REQUEST_SIZE + " ± " + REQUEST_SIZE_STD_DEV + " KB");
        System.out.println("  - Computation length: " + PMU_DATA_LENGTH + " MI (data transfer only)");
    }
    
    /**
     * Reads PMU generation rate from applications.xml
     */
    private int readPmuRateFromApplications() {
        try {
            // Get the first application's rate (PMU_Data)
            if (!SimulationParameters.applicationList.isEmpty()) {
                int ratePerMinute = SimulationParameters.applicationList.get(0).getRate();
                // **NOTE: Rate in XML is now measurements per second**
                System.out.printf("PmuTaskGenerator - DEBUG: Read rate from applications.xml: %d tasks/second%n", ratePerMinute);
                return ratePerMinute;
            } else {
                System.err.println("PmuTaskGenerator - ERROR: applicationList is empty!");
            }
        } catch (Exception e) {
            System.err.println("PmuTaskGenerator - Error reading rate from applications.xml: " + e.getMessage());
            e.printStackTrace();
        }
        
        // Fallback to 1 measurement per second
        System.out.println("PmuTaskGenerator - WARNING: Using fallback rate: 1 measurement/second");
        return 1;
    }
    
    @Override
    public FutureQueue<Task> generate() {
        if (simulationManager == null) return taskList;
        
        devicesList.removeIf(dev -> !dev.isGeneratingTasks());
        
        if (devicesList.isEmpty()) {
            simulationManager.getSimulationLogger().print("Warning: No PMU devices found that generate data!");
            return taskList;
        }
        
        double simulationTime = SimulationParameters.simulationDuration;
        int totalSeconds = (int) simulationTime;
        
        // **FIXED: Generate synchronized PMU measurements based on TASKS_PER_SECOND rate**
        for (int second = 0; second < totalSeconds; second++) {
            // **NEW: Generate multiple measurements per second based on rate**
            for (int measurement = 0; measurement < TASKS_PER_SECOND; measurement++) {
                // **IMPORTANT: All PMUs generate measurements at EXACTLY the same time for synchronization**
                double exactTime = (double) second + (measurement * (1.0 / TASKS_PER_SECOND)); // Spread measurements within the second
                
                // **DEBUG: Log first few fractional times for verification**
                if (second == 0 && measurement < 3) {
                    System.out.printf("PmuTaskGenerator - DEBUG: Creating tasks for time %.3f (second=%d, measurement=%d/%d)%n", 
                                     exactTime, second, measurement, TASKS_PER_SECOND);
                }
                
                // For each PMU device, generate measurement at exact time
                for (ComputingNode pmu : devicesList) {
                    long dataSize = (long) Math.max(1, requestSizeDistribution.sample()); // Variable data size
                    insertPmuDataTask(exactTime, 0, pmu, PMU_DATA_LENGTH, dataSize * 8192); // Convert to bits
                }
            }
        }
        
        System.out.printf("PmuTaskGenerator - Generated %d synchronized PMU data tasks for %d PMUs over %d seconds at %d measurements/second%n",
                         taskList.size(), devicesList.size(), totalSeconds, TASKS_PER_SECOND);
        
        // **DEBUG: Verify expected task count**
        int expectedTasks = devicesList.size() * totalSeconds * TASKS_PER_SECOND;
        System.out.printf("PmuTaskGenerator - Expected: %d tasks, Actual: %d tasks, Match: %s%n",
                         expectedTasks, taskList.size(), expectedTasks == taskList.size() ? "YES" : "NO");
        
        return taskList;
    }
    
    /**
     * Creates a PMU data transfer task (not a computational task)
     */
    protected void insertPmuDataTask(double time, int appId, ComputingNode pmuDevice, long dataLength, long dataSize) {
        if (time > SimulationParameters.simulationDuration) {
            time = SimulationParameters.simulationDuration - 0.1;
        }
        
        try {
            // **FIXED: NO OUTPUT SIZE for pure data transfers - prevents return cycles**
            long outputSize = 0; // NO return data from PMU transfers
            long containerSize = SimulationParameters.applicationList.get(appId).getContainerSizeInBits();
            double maxLatency = SimulationParameters.applicationList.get(appId).getLatency();
            String type = SimulationParameters.applicationList.get(appId).getType();
            
            // **UPDATED: Create PMU data transfer task with NO return results**
            Task dataTask = createTask(++id)
                    .setType("PMU_DATA:" + type) // Mark as PMU data transfer
                    .setFileSizeInBits(dataSize) // PMU measurement data size
                    .setOutputSizeInBits(outputSize) // **FIXED: 0 - No return data**
                    .setContainerSizeInBits(containerSize)
                    .setApplicationID(appId)
                    .setMaxLatency(maxLatency)
                    .setLength(dataLength) // **SET TO 0 - No computation for data transfer**
                    .setEdgeDevice(pmuDevice) // Source PMU
                    .setRegistry(getSimulationManager().getDataCentersManager()
                    .getComputingNodesGenerator().getCloudOnlyList().get(0)); // TSO registry
            
            dataTask.setTime(time);
            taskList.add(dataTask);
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
} 