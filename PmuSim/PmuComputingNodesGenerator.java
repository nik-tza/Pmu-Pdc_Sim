package PmuSim;

import com.mechalikh.pureedgesim.datacentersmanager.ComputingNode;
import com.mechalikh.pureedgesim.datacentersmanager.DefaultComputingNodesGenerator;
import com.mechalikh.pureedgesim.locationmanager.MobilityModel;
import com.mechalikh.pureedgesim.simulationmanager.SimulationManager;

/**
 * PMU Computing Nodes Generator for Smart Grid simulation.
 * Extends DefaultComputingNodesGenerator to read from XML files
 * and applies PMU-specific configurations for sensor devices.
 */
public class PmuComputingNodesGenerator extends DefaultComputingNodesGenerator {

    public PmuComputingNodesGenerator(SimulationManager simulationManager,
            Class<? extends MobilityModel> mobilityModelClass, Class<? extends ComputingNode> computingNodeClass) {
        super(simulationManager, mobilityModelClass, computingNodeClass);
    }

    @Override
    protected void insertEdgeDevice(ComputingNode newDevice) {
        super.insertEdgeDevice(newDevice);
        
        // Apply PMU-specific configurations for sensor devices
        configurePmuDevice(newDevice);
    }
    
    /**
     * Applies PMU-specific configurations to devices.
     */
    private void configurePmuDevice(ComputingNode device) {
        if (device.isGeneratingTasks()) {
            // Configure as PMU sensor device
            device.setAsSensor(true);
            device.enableTaskGeneration(true);
            
            getSimulationManager().getSimulationLogger().print(
                "PmuComputingNodesGenerator - Configured PMU sensor device " + 
                device.getId() + " at position (" + 
                device.getMobilityModel().getCurrentLocation().getXPos() + ", " +
                device.getMobilityModel().getCurrentLocation().getYPos() + ")" +
                " with " + device.getTotalMipsCapacity() + " MIPS, " +
                device.getRamCapacity() + " MB RAM, " +
                device.getTotalStorage() + " MB storage");
        }
    }
} 