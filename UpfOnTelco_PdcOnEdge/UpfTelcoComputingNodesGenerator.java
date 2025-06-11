package UpfOnTelco_PdcOnEdge;

import com.mechalikh.pureedgesim.datacentersmanager.ComputingNode;
import com.mechalikh.pureedgesim.datacentersmanager.DefaultComputingNodesGenerator;
import com.mechalikh.pureedgesim.locationmanager.MobilityModel;
import com.mechalikh.pureedgesim.simulationmanager.SimulationManager;
import com.mechalikh.pureedgesim.scenariomanager.SimulationParameters;
import com.mechalikh.pureedgesim.energy.EnergyModelComputingNode;
import com.mechalikh.pureedgesim.locationmanager.Location;

import org.w3c.dom.Element;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Random;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * UpfTelco Computing Nodes Generator for Smart Grid simulation.
 * Extends DefaultComputingNodesGenerator to read from XML files
 * and applies UpfTelco-specific configurations for sensor devices.
 */
public class UpfTelcoComputingNodesGenerator extends DefaultComputingNodesGenerator {

    private static Long cachedPmuPlacementSeed = null;
    private static int pmuCounter = 0;
    
    public UpfTelcoComputingNodesGenerator(SimulationManager simulationManager,
            Class<? extends MobilityModel> mobilityModelClass, Class<? extends ComputingNode> computingNodeClass) {
        super(simulationManager, mobilityModelClass, computingNodeClass);
        // Reset cache and counter for new simulation run
        cachedPmuPlacementSeed = null;
        pmuCounter = 0;
    }
    
    /**
     * **NEW: Read PMU placement seed directly from properties file**
     */
    private long getPmuPlacementSeed() {
        if (cachedPmuPlacementSeed != null) {
            return cachedPmuPlacementSeed;
        }
        
        try (FileInputStream input = new FileInputStream("UpfOnTelco_PdcOnEdge/settings/simulation_parameters.properties")) {
            Properties prop = new Properties();
            prop.load(input);
            String seedValue = prop.getProperty("pmu_placement_seed", "12345");
            cachedPmuPlacementSeed = Long.parseLong(seedValue.trim());
            
            getSimulationManager().getSimulationLogger().deepLog(
                "UpfTelcoComputingNodesGenerator - Loaded PMU placement seed: " + cachedPmuPlacementSeed);
                
            return cachedPmuPlacementSeed;
        } catch (IOException | NumberFormatException e) {
            getSimulationManager().getSimulationLogger().print(
                "UpfTelcoComputingNodesGenerator - Warning: Could not read pmu_placement_seed, using default 12345");
            cachedPmuPlacementSeed = 12345L;
            return cachedPmuPlacementSeed;
        }
    }

    @Override
    protected void insertEdgeDevice(ComputingNode newDevice) {
        super.insertEdgeDevice(newDevice);
        
        // Apply UpfTelco-specific configurations for sensor devices
        configureUpfTelcoDevice(newDevice);
    }
    
    /**
     * Applies UpfTelco-specific configurations to devices.
     */
    private void configureUpfTelcoDevice(ComputingNode device) {
        if (device.isGeneratingTasks()) {
            // Configure as UpfTelco sensor device
            device.setAsSensor(true);
            device.enableTaskGeneration(true);
            
            getSimulationManager().getSimulationLogger().print(
                "UpfTelcoComputingNodesGenerator - Configured UpfTelco sensor device " + 
                device.getId() + " at position (" + 
                device.getMobilityModel().getCurrentLocation().getXPos() + ", " +
                device.getMobilityModel().getCurrentLocation().getYPos() + ")" +
                " with " + device.getTotalMipsCapacity() + " MIPS, " +
                device.getRamCapacity() + " MB RAM, " +
                device.getTotalStorage() + " MB storage");
        }
    }

    /**
     * **NEW: Override createComputingNode to implement seeded random PMU placement**
     */
    @Override
    protected ComputingNode createComputingNode(Element datacenterElement, SimulationParameters.TYPES type)
            throws NoSuchAlgorithmException, NoSuchMethodException, SecurityException, InstantiationException,
            IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        
        // For EDGE_DEVICE type (PMUs), use our custom placement logic
        if (type == SimulationParameters.TYPES.EDGE_DEVICE) {
            return createSeededUpfTelcoDevice(datacenterElement, type);
        } else {
            // For other types, use the default implementation
            return super.createComputingNode(datacenterElement, type);
        }
    }

    /**
     * **NEW: Create UpfTelco device with seeded random placement**
     */
    private ComputingNode createSeededUpfTelcoDevice(Element datacenterElement, SimulationParameters.TYPES type)
            throws NoSuchAlgorithmException, NoSuchMethodException, SecurityException, InstantiationException,
            IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        
        // **SEEDED RANDOM: Use seeded Random for PMU placement reproducibility**
        Random random;
        long pmuSeed = getPmuPlacementSeed();
        if (pmuSeed != -1) {
            // Use seeded Random for reproducible PMU placement - create deterministic seed per PMU
            // Each PMU gets a different but deterministic seed based on the base seed and PMU index
            long uniqueSeed = pmuSeed * 1000000L + pmuCounter;
            random = new Random(uniqueSeed);
            
            getSimulationManager().getSimulationLogger().deepLog(
                "UpfTelcoComputingNodesGenerator - Using seeded random for PMU " + pmuCounter + 
                " with base seed: " + pmuSeed + ", unique seed: " + uniqueSeed);
            
            pmuCounter++; // Increment counter for next PMU
        } else {
            // SecureRandom for truly random placement
            random = SecureRandom.getInstanceStrong();
            getSimulationManager().getSimulationLogger().deepLog(
                "UpfTelcoComputingNodesGenerator - Using secure random for PMU " + pmuCounter);
            pmuCounter++;
        }

        // Parse device configuration from XML
        Boolean mobile = Boolean.parseBoolean(datacenterElement.getElementsByTagName("mobility").item(0).getTextContent());
        double speed = Double.parseDouble(datacenterElement.getElementsByTagName("speed").item(0).getTextContent());
        double minPauseDuration = Double.parseDouble(datacenterElement.getElementsByTagName("minPauseDuration").item(0).getTextContent());
        double maxPauseDuration = Double.parseDouble(datacenterElement.getElementsByTagName("maxPauseDuration").item(0).getTextContent());
        double minMobilityDuration = Double.parseDouble(datacenterElement.getElementsByTagName("minMobilityDuration").item(0).getTextContent());
        double maxMobilityDuration = Double.parseDouble(datacenterElement.getElementsByTagName("maxMobilityDuration").item(0).getTextContent());
        double idleConsumption = Double.parseDouble(datacenterElement.getElementsByTagName("idleConsumption").item(0).getTextContent());
        double maxConsumption = Double.parseDouble(datacenterElement.getElementsByTagName("maxConsumption").item(0).getTextContent());
        int numOfCores = Integer.parseInt(datacenterElement.getElementsByTagName("cores").item(0).getTextContent());
        double mips = Double.parseDouble(datacenterElement.getElementsByTagName("mips").item(0).getTextContent());
        double storage = Double.parseDouble(datacenterElement.getElementsByTagName("storage").item(0).getTextContent());
        double ram = Double.parseDouble(datacenterElement.getElementsByTagName("ram").item(0).getTextContent());

        // Create the computing node
        Constructor<?> datacenterConstructor = getComputingNodeClass().getConstructor(SimulationManager.class, double.class, int.class, double.class, double.class);
        ComputingNode computingNode = (ComputingNode) datacenterConstructor.newInstance(getSimulationManager(), mips, numOfCores, storage, ram);

        // Set orchestrator properties
        computingNode.setAsOrchestrator(Boolean.parseBoolean(datacenterElement.getElementsByTagName("isOrchestrator").item(0).getTextContent()));
        
        // Set energy model
        computingNode.setEnergyModel(new EnergyModelComputingNode(maxConsumption, idleConsumption));
        
        // Configure battery properties
        computingNode.getEnergyModel().setBattery(Boolean.parseBoolean(datacenterElement.getElementsByTagName("battery").item(0).getTextContent()));
        computingNode.getEnergyModel().setBatteryCapacity(Double.parseDouble(datacenterElement.getElementsByTagName("batteryCapacity").item(0).getTextContent()));
        computingNode.getEnergyModel().setIntialBatteryPercentage(Double.parseDouble(datacenterElement.getElementsByTagName("initialBatteryLevel").item(0).getTextContent()));
        computingNode.getEnergyModel().setConnectivityType(datacenterElement.getElementsByTagName("connectivity").item(0).getTextContent());
        computingNode.enableTaskGeneration(Boolean.parseBoolean(datacenterElement.getElementsByTagName("generateTasks").item(0).getTextContent()));
        
        // **NEW: Shuffled grid-based PMU placement with seeded randomness**
        // Reads map dimensions from properties, creates grid, shuffles cell order based on seed
        // Much more random while maintaining uniform distribution and reproducibility
        Location datacenterLocation = generateSmartPmuLocation(random);
        
        getSimulationManager().getSimulationLogger().deepLog(
            "UpfTelcoComputingNodesGenerator - PMU device " + getMistOnlyList().size() + 
            " location: (" + datacenterLocation.getXPos() + "," + datacenterLocation.getYPos() + ")"
        );

        // Set device type
        computingNode.setType(type);
        
        // Create mobility model
        Constructor<?> mobilityConstructor = getMobilityModelClass().getConstructor(SimulationManager.class, Location.class);
        MobilityModel mobilityModel = ((MobilityModel) mobilityConstructor.newInstance(getSimulationManager(), datacenterLocation))
            .setMobile(mobile)
            .setSpeed(speed)
            .setMinPauseDuration(minPauseDuration)
            .setMaxPauseDuration(maxPauseDuration)
            .setMinMobilityDuration(minMobilityDuration)
            .setMaxMobilityDuration(maxMobilityDuration);

        computingNode.setMobilityModel(mobilityModel);

        return computingNode;
    }

    /**
     * **NEW: Shuffled grid-based PMU placement with seeded randomness**
     * Reads map dimensions from properties, creates grid, shuffles cell order based on seed
     * Much more random while maintaining uniform distribution and reproducibility
     */
    private Location generateSmartPmuLocation(Random random) {
        // Get map dimensions from SimulationParameters (loaded from properties)
        int mapWidth = SimulationParameters.simulationMapWidth;
        int mapLength = SimulationParameters.simulationMapLength;
        int totalPmus = SimulationParameters.maxNumberOfEdgeDevices;
        int currentPmuIndex = pmuCounter - 1; // -1 because pmuCounter already incremented
        
        // Create a grid-based distribution
        // Calculate grid dimensions to distribute PMUs roughly evenly
        int gridCols = (int) Math.ceil(Math.sqrt(totalPmus * (double)mapWidth / mapLength));
        int gridRows = (int) Math.ceil((double)totalPmus / gridCols);
        
        // Calculate cell dimensions
        double cellWidth = (double)mapWidth / gridCols;
        double cellHeight = (double)mapLength / gridRows;
        
        // **NEW: Create shuffled list of grid cells for more randomness**
        // Create list of all grid positions
        java.util.List<int[]> gridCells = new java.util.ArrayList<>();
        for (int row = 0; row < gridRows; row++) {
            for (int col = 0; col < gridCols; col++) {
                gridCells.add(new int[]{row, col});
            }
        }
        
        // Shuffle the grid cells using the same seed-based approach for reproducibility
        long shuffleSeed = getPmuPlacementSeed() * 1000L + 999L; // Different but deterministic seed
        java.util.Random shuffleRandom = new java.util.Random(shuffleSeed);
        java.util.Collections.shuffle(gridCells, shuffleRandom);
        
        // Get the shuffled grid position for this PMU
        if (currentPmuIndex >= gridCells.size()) {
            // Fallback for edge case where we have more PMUs than grid cells
            currentPmuIndex = currentPmuIndex % gridCells.size();
        }
        
        int[] assignedCell = gridCells.get(currentPmuIndex);
        int gridRow = assignedCell[0];
        int gridCol = assignedCell[1];
        
        // Calculate cell boundaries
        double cellMinX = gridCol * cellWidth;
        double cellMaxX = Math.min(cellMinX + cellWidth, mapWidth - 1);
        double cellMinY = gridRow * cellHeight;
        double cellMaxY = Math.min(cellMinY + cellHeight, mapLength - 1);
        
        // Place PMU randomly within the assigned cell
        double x = cellMinX + random.nextDouble() * (cellMaxX - cellMinX);
        double y = cellMinY + random.nextDouble() * (cellMaxY - cellMinY);
        
        return new Location(x, y);
    }
    
    protected Class<? extends ComputingNode> getComputingNodeClass() {
        return computingNodeClass;
    }
    
    protected Class<? extends MobilityModel> getMobilityModelClass() {
        return mobilityModelClass;
    }
} 