package PmuSim;
import com.mechalikh.pureedgesim.simulationmanager.Simulation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.InputStreamReader;
import java.util.Arrays;

/**
 * Main class for executing the PMU Smart Grid simulation.
 * This simulation implements a sensor-only PMU architecture that reads configuration
 * from XML files and uses default PureEdgeSim components where possible.
 * 
 * Architecture: PMU sensors generate tasks that are processed by edge datacenters
 * using the default PureEdgeSim orchestration and network models.
 */
public class PmuSimulation {
    // Define paths for settings and output folders
    private static final String SETTINGS_PATH = "PmuSim/Pmu_settings/";
    private static final String OUTPUT_PATH = "PmuSim/Pmu_output/";

    public PmuSimulation() {
        // Create output folder if it doesn't exist
        File outputDir = new File(OUTPUT_PATH);
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        try {
            // Initialize the PMU logger
            PmuLogger.initialize(OUTPUT_PATH);
            
            // Create the simulation with all its required components
            Simulation sim = new Simulation();
            
            // Set custom folders
            sim.setCustomOutputFolder(OUTPUT_PATH);
            sim.setCustomSettingsFolder(SETTINGS_PATH);
            
            // Set custom components
            sim.setCustomTaskGenerator(PmuTaskGenerator.class);
            sim.setCustomComputingNodesGenerator(PmuComputingNodesGenerator.class);
            sim.setCustomSimulationManager(PmuSimulationManager.class);
            sim.setCustomEdgeOrchestrator(PmuTaskOrchestrator.class);
            sim.setCustomNetworkModel(PmuNetworkModel.class);   
            
            // Start the simulation
            System.out.println("PmuSimulation - Starting PMU Smart Grid data-based simulation...");
            sim.launchSimulation();
            
            // Diagnostic message after simulation
            System.out.println("PmuSimulation - Simulation completed, saving logs...");
            
            // Save logs to CSV
            PmuLogger logger = PmuLogger.getInstance();
            if (logger != null) {
                logger.saveAllLogs();
                System.out.println("PmuSimulation - Logs saved successfully.");
            } else {
                System.err.println("PmuSimulation - Error: PmuLogger instance is null!");
            }
            
            // Execute LogAnalysis.py for PMU simulation
            executePmuAnalysis();
            
        } catch (Exception e) {
            System.err.println("Error during simulation: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Executes the PMU analysis Python script
     */
    private void executePmuAnalysis() {
        try {
            // Find the most recently created folder
            File[] folders = new File(OUTPUT_PATH).listFiles(File::isDirectory);
            if (folders != null && folders.length > 0) {
                // Sort by last modified date
                Arrays.sort(folders, (a, b) -> Long.compare(b.lastModified(), a.lastModified()));
                String latestFolder = folders[0].getName();
                
                // Path for Python script
                String pythonScript = "PmuSim/PmuLogAnalysis.py";
                File scriptFile = new File(pythonScript);
                
                if (!scriptFile.exists()) {
                    System.err.println("PmuLogAnalysis.py file not found at: " + pythonScript);
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
                System.out.println("\n=== Output from PmuLogAnalysis.py ===");
                while ((line = reader.readLine()) != null) {
                    System.out.println(line);
                }
                
                // Print errors if any
                boolean hasErrors = false;
                while ((line = errorReader.readLine()) != null) {
                    System.err.println("Error from PmuLogAnalysis.py: " + line);
                    hasErrors = true;
                }
                
                if (hasErrors) {
                    System.err.println("There were errors during PmuLogAnalysis.py execution");
                }
                
                process.waitFor();
            } else {
                System.err.println("No output folder found for analysis");
            }
        } catch (Exception e) {
            System.err.println("Error while executing PmuLogAnalysis.py:");
            e.printStackTrace();
        }
    }

    /**
     * Main method to run the PMU Smart Grid simulation
     */
    public static void main(String[] args) {
        System.out.println("=== PMU Smart Grid Simulation ===");
        new PmuSimulation();
        System.out.println("=== PMU Simulation Complete ===");
    }
}