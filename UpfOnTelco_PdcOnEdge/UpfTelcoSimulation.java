package UpfOnTelco_PdcOnEdge;
import com.mechalikh.pureedgesim.simulationmanager.Simulation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.InputStreamReader;
import java.util.Arrays;

/**
 * Main class for executing the UpfTelco Smart Grid simulation.
 * This simulation implements a sensor-only UpfTelco architecture that reads configuration
 * from XML files and uses default PureEdgeSim components where possible.
 * 
 * Architecture: UpfTelco sensors generate tasks that are processed by edge datacenters
 * using the default PureEdgeSim orchestration and network models.
 */
public class UpfTelcoSimulation {
    // Define paths for settings and output folders
    private static final String SETTINGS_PATH = "UpfOnTelco_PdcOnEdge/settings/";
    private static final String OUTPUT_PATH = "UpfOnTelco_PdcOnEdge/output/";

    public UpfTelcoSimulation() {
        // Create output folder if it doesn't exist
        File outputDir = new File(OUTPUT_PATH);
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        try {
            // Force set the output folder in SimulationParameters before initializing logger
            com.mechalikh.pureedgesim.scenariomanager.SimulationParameters.outputFolder = OUTPUT_PATH;
            System.out.println("DEBUG UpfOnTelco_PdcOnEdge: Forced SimulationParameters.outputFolder to: " + 
                              com.mechalikh.pureedgesim.scenariomanager.SimulationParameters.outputFolder);
            
            // Initialize the UpfTelco logger
            UpfTelcoLogger.initialize(OUTPUT_PATH);
            
            // Create the simulation with all its required components
            Simulation sim = new Simulation();
            
            // Set custom folders
            System.out.println("DEBUG UpfOnTelco_PdcOnEdge: Setting output folder to: " + OUTPUT_PATH);
            sim.setCustomOutputFolder(OUTPUT_PATH);
            sim.setCustomSettingsFolder(SETTINGS_PATH);
            System.out.println("DEBUG UpfOnTelco_PdcOnEdge: After setCustomOutputFolder, SimulationParameters.outputFolder = " + 
                              com.mechalikh.pureedgesim.scenariomanager.SimulationParameters.outputFolder);
            
            // Set custom components
            sim.setCustomTaskGenerator(UpfTelcoTaskGenerator.class);
            sim.setCustomComputingNodesGenerator(UpfTelcoComputingNodesGenerator.class);
            sim.setCustomSimulationManager(UpfTelcoSimulationManager.class);
            sim.setCustomEdgeOrchestrator(UpfTelcoTaskOrchestrator.class);
            sim.setCustomNetworkModel(UpfTelcoNetworkModel.class);   
            
            // Start the simulation
            System.out.println("UpfTelcoSimulation - Starting UpfTelco Smart Grid data-based simulation...");
            sim.launchSimulation();
            
            // Diagnostic message after simulation
            System.out.println("UpfTelcoSimulation - Simulation completed, saving logs...");
            
            // Save logs to CSV
            UpfTelcoLogger logger = UpfTelcoLogger.getInstance();
            if (logger != null) {
                logger.saveAllLogs();
                System.out.println("UpfTelcoSimulation - Logs saved successfully.");
            } else {
                System.err.println("UpfTelcoSimulation - Error: UpfTelcoLogger instance is null!");
            }
            
            // Execute LogAnalysis.py for UpfTelco simulation
            executeUpfTelcoAnalysis();
            
        } catch (Exception e) {
            System.err.println("Error during simulation: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Executes the UpfTelco analysis Python script
     */
    private void executeUpfTelcoAnalysis() {
        try {
            // Find the most recently created folder
            File[] folders = new File(OUTPUT_PATH).listFiles(File::isDirectory);
            if (folders != null && folders.length > 0) {
                // Sort by last modified date
                Arrays.sort(folders, (a, b) -> Long.compare(b.lastModified(), a.lastModified()));
                String latestFolder = folders[0].getName();
                
                // Path for Python script
                String pythonScript = "UpfOnTelco_PdcOnEdge/UpfTelcoLogAnalysis.py";
                File scriptFile = new File(pythonScript);
                
                if (!scriptFile.exists()) {
                    System.err.println("UpfTelcoLogAnalysis.py file not found at: " + pythonScript);
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
                System.out.println("\n=== Output from UpfTelcoLogAnalysis.py ===");
                while ((line = reader.readLine()) != null) {
                    System.out.println(line);
                }
                
                // Print errors if any
                boolean hasErrors = false;
                while ((line = errorReader.readLine()) != null) {
                    System.err.println("Error from UpfTelcoLogAnalysis.py: " + line);
                    hasErrors = true;
                }
                
                if (hasErrors) {
                    System.err.println("There were errors during UpfTelcoLogAnalysis.py execution");
                }
                
                process.waitFor();
            } else {
                System.err.println("No output folder found for analysis");
            }
        } catch (Exception e) {
            System.err.println("Error while executing UpfTelcoLogAnalysis.py:");
            e.printStackTrace();
        }
    }

    /**
     * Main method to run the UpfTelco Smart Grid simulation
     */
    public static void main(String[] args) {
        System.out.println("=== UpfTelco Smart Grid Simulation ===");
        new UpfTelcoSimulation();
        System.out.println("=== UpfTelco Simulation Complete ===");
    }
}