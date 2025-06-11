package UpfOnEdge_PdcOnEdge;
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
public class EdgeSimulation {
    // Define paths for settings and output folders
    private static final String SETTINGS_PATH = "UpfOnEdge_PdcOnEdge/settings/";
    private static final String OUTPUT_PATH = "UpfOnEdge_PdcOnEdge/output/";

    public EdgeSimulation() {
        // Create output folder if it doesn't exist
        File outputDir = new File(OUTPUT_PATH);
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        try {
            // Force set the output folder in SimulationParameters before initializing logger
            com.mechalikh.pureedgesim.scenariomanager.SimulationParameters.outputFolder = OUTPUT_PATH;
            System.out.println("DEBUG UpfOnEdge_PdcOnEdge: Forced SimulationParameters.outputFolder to: " + 
                              com.mechalikh.pureedgesim.scenariomanager.SimulationParameters.outputFolder);
            
            // Initialize the Edge logger
            EdgeLogger.initialize(OUTPUT_PATH);
            
            // Create the simulation with all its required components
            Simulation sim = new Simulation();
            
            // Set custom folders
            System.out.println("DEBUG UpfOnEdge_PdcOnEdge: Setting output folder to: " + OUTPUT_PATH);
            sim.setCustomOutputFolder(OUTPUT_PATH);
            sim.setCustomSettingsFolder(SETTINGS_PATH);
            System.out.println("DEBUG UpfOnEdge_PdcOnEdge: After setCustomOutputFolder, SimulationParameters.outputFolder = " + 
                              com.mechalikh.pureedgesim.scenariomanager.SimulationParameters.outputFolder);
            
            // Set custom components
            sim.setCustomTaskGenerator(EdgeTaskGenerator.class);
            sim.setCustomComputingNodesGenerator(EdgeComputingNodesGenerator.class);
            sim.setCustomSimulationManager(EdgeSimulationManager.class);
            sim.setCustomEdgeOrchestrator(EdgeTaskOrchestrator.class);
            sim.setCustomNetworkModel(EdgeNetworkModel.class);   
            
            // Start the simulation
            System.out.println("EdgeSimulation - Starting Edge Smart Grid data-based simulation...");
            sim.launchSimulation();
            
            // Diagnostic message after simulation
            System.out.println("EdgeSimulation - Simulation completed, saving logs...");
            
            // Save logs to CSV
            EdgeLogger logger = EdgeLogger.getInstance();
            if (logger != null) {
                logger.saveAllLogs();
                System.out.println("EdgeSimulation - Logs saved successfully.");
            } else {
                System.err.println("EdgeSimulation - Error: EdgeLogger instance is null!");
            }
            
            // Execute LogAnalysis.py for Edge simulation
            executeEdgeAnalysis();
            
        } catch (Exception e) {
            System.err.println("Error during simulation: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Executes the Edge analysis Python script
     */
    private void executeEdgeAnalysis() {
        try {
            // Find the most recently created folder
            File[] folders = new File(OUTPUT_PATH).listFiles(File::isDirectory);
            if (folders != null && folders.length > 0) {
                // Sort by last modified date
                Arrays.sort(folders, (a, b) -> Long.compare(b.lastModified(), a.lastModified()));
                String latestFolder = folders[0].getName();
                
                // Path for Python script
                String pythonScript = "UpfOnEdge_PdcOnEdge/EdgeLogAnalysis.py";
                File scriptFile = new File(pythonScript);
                
                if (!scriptFile.exists()) {
                    System.err.println("EdgeLogAnalysis.py file not found at: " + pythonScript);
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
                System.out.println("\n=== Output from EdgeLogAnalysis.py ===");
                while ((line = reader.readLine()) != null) {
                    System.out.println(line);
                }
                
                // Print errors if any
                boolean hasErrors = false;
                while ((line = errorReader.readLine()) != null) {
                    System.err.println("Error from EdgeLogAnalysis.py: " + line);
                    hasErrors = true;
                }
                
                if (hasErrors) {
                    System.err.println("There were errors during EdgeLogAnalysis.py execution");
                }
                
                process.waitFor();
            } else {
                System.err.println("No output folder found for analysis");
            }
        } catch (Exception e) {
            System.err.println("Error while executing EdgeLogAnalysis.py:");
            e.printStackTrace();
        }
    }

    /**
     * Main method to run the Edge Smart Grid simulation
     */
    public static void main(String[] args) {
        System.out.println("=== Edge Smart Grid Simulation ===");
        new EdgeSimulation();
        System.out.println("=== Edge Simulation Complete ===");
    }
}