package UpfOnTelco_PdcOnCloud;
import com.mechalikh.pureedgesim.simulationmanager.Simulation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.InputStreamReader;
import java.util.Arrays;

/**
 * Main class for executing the Edge Smart Grid simulation.
 * This simulation implements a sensor-only Edge architecture that reads configuration
 * from XML files and uses default PureEdgeSim components where possible.
 * 
 * Architecture: Edge sensors generate tasks that are processed by edge datacenters
 * using the default PureEdgeSim orchestration and network models.
 */
public class CloudSimulation {
    // Define paths for settings and output folders
    private static final String SETTINGS_PATH = "UpfOnTelco_PdcOnCloud/settings/";
    private static final String OUTPUT_PATH = "UpfOnTelco_PdcOnCloud/output/";

    public CloudSimulation() {
        // Create output folder if it doesn't exist
        File outputDir = new File(OUTPUT_PATH);
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        try {
            // Initialize the Cloud logger
            // CloudLogger.initialize(OUTPUT_PATH);
            
            // Create the simulation with all its required components
            Simulation sim = new Simulation();
            
            // Set custom folders
            System.out.println("DEBUG UpfTelcoComputingNodesGenerator: Setting output folder to: " + OUTPUT_PATH);
            sim.setCustomOutputFolder(OUTPUT_PATH);
            sim.setCustomSettingsFolder(SETTINGS_PATH);
            System.out.println("DEBUG UpfTelcoComputingNodesGenerator: After setCustomOutputFolder, SimulationParameters.outputFolder = " + 
                              com.mechalikh.pureedgesim.scenariomanager.SimulationParameters.outputFolder);
            
            // Set custom components
            sim.setCustomTaskGenerator(CloudTaskGenerator.class);
            sim.setCustomComputingNodesGenerator(CloudComputingNodesGenerator.class);
            sim.setCustomSimulationManager(CloudSimulationManager.class);
            sim.setCustomEdgeOrchestrator(CloudTaskOrchestrator.class);
            sim.setCustomNetworkModel(CloudNetworkModel.class);   
            
            // Start the simulation
            System.out.println("CloudSimulation - Starting Cloud Smart Grid data-based simulation...");
            sim.launchSimulation();
            
            // Diagnostic message after simulation
            System.out.println("CloudSimulation - Simulation completed, saving logs...");
            
            // Save logs to CSV
            CloudLogger logger = CloudLogger.getInstance();
            if (logger != null) {
                logger.saveAllLogs();
                System.out.println("CloudSimulation - Logs saved successfully.");
            } else {
                System.err.println("CloudSimulation - Error: CloudLogger instance is null!");
            }
            
            // Execute LogAnalysis.py for Cloud simulation
            executeCloudAnalysis();
            
        } catch (Exception e) {
            System.err.println("Error during simulation: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Executes the Cloud analysis Python script
     */
    private void executeCloudAnalysis() {
        try {
            // Find the most recently created folder
            File[] folders = new File(OUTPUT_PATH).listFiles(File::isDirectory);
            if (folders != null && folders.length > 0) {
                // Sort by last modified date
                Arrays.sort(folders, (a, b) -> Long.compare(b.lastModified(), a.lastModified()));
                String latestFolder = folders[0].getName();
                
                // Path for Python script
                String pythonScript = "UpfOnTelco_PdcOnCloud/CloudLogAnalysis.py";
                File scriptFile = new File(pythonScript);
                
                if (!scriptFile.exists()) {
                    System.err.println("CloudLogAnalysis.py file not found at: " + pythonScript);
                    return;
                }
                
                // Construct the command to execute the Python script
                String command = String.format("python3 %s %s", pythonScript, latestFolder);
                System.out.println("Executing command: " + command);
                
                // Execute the command
                Process process = Runtime.getRuntime().exec(command);
                
                // Print output
                BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                BufferedReader errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
                
                String line;
                System.out.println("\n=== Output from CloudLogAnalysis.py ===");
                while ((line = reader.readLine()) != null) {
                    System.out.println(line);
                }
                
                // Print errors if any
                boolean hasErrors = false;
                while ((line = errorReader.readLine()) != null) {
                    System.err.println("Error from CloudLogAnalysis.py: " + line);
                    hasErrors = true;
                }
                
                if (hasErrors) {
                    System.err.println("There were errors during CloudLogAnalysis.py execution");
                }
                
                process.waitFor();
            } else {
                System.err.println("No output folder found for analysis");
            }
        } catch (Exception e) {
            System.err.println("Error while executing CloudLogAnalysis.py:");
            e.printStackTrace();
        }
    }

    /**
     * Main method to run the Cloud Smart Grid simulation
     */
    public static void main(String[] args) {
        System.out.println("=== Cloud Smart Grid Simulation ===");
        new CloudSimulation();
        System.out.println("=== Cloud Simulation Complete ===");
    }
}