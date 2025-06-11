#!/bin/bash

# Define parameters we want to change for Edge Smart Grid simulation
NUM_DEVICES=30
SIMULATION_TIME=20
PMU_RATE=3  # PMU measurements per second (frequency)
# Map and coverage parameters for PMU grid
MAP_LENGTH=2000
MAP_WIDTH=2000
EDGE_DEVICES_RANGE=20
EDGE_DATACENTERS_COVERAGE=800

# Change parameters in simulation_parameters.properties for UpfOnEdge_PdcOnEdge
sed -i "/^min_number_of_edge_devices=/s/min_number_of_edge_devices=.*/min_number_of_edge_devices=$NUM_DEVICES/" UpfOnEdge_PdcOnEdge/settings/simulation_parameters.properties
sed -i "/^max_number_of_edge_devices=/s/max_number_of_edge_devices=.*/max_number_of_edge_devices=$NUM_DEVICES/" UpfOnEdge_PdcOnEdge/settings/simulation_parameters.properties
sed -i "/^simulation_time=/s/simulation_time=.*/simulation_time=$SIMULATION_TIME/" UpfOnEdge_PdcOnEdge/settings/simulation_parameters.properties
# Map and coverage parameters
sed -i "/^length=/s/length=.*/length=$MAP_LENGTH/" UpfOnEdge_PdcOnEdge/settings/simulation_parameters.properties
sed -i "/^width=/s/width=.*/width=$MAP_WIDTH/" UpfOnEdge_PdcOnEdge/settings/simulation_parameters.properties
sed -i "/^edge_devices_range=/s/edge_devices_range=.*/edge_devices_range=$EDGE_DEVICES_RANGE/" UpfOnEdge_PdcOnEdge/settings/simulation_parameters.properties
sed -i "/^edge_datacenters_coverage=/s/edge_datacenters_coverage=.*/edge_datacenters_coverage=$EDGE_DATACENTERS_COVERAGE/" UpfOnEdge_PdcOnEdge/settings/simulation_parameters.properties
sed -i "s|<rate>[0-9]*</rate>|<rate>$PMU_RATE</rate>|g" UpfOnEdge_PdcOnEdge/settings/applications.xml



# Execute mvn clean install
echo "Executing mvn clean install for Edge PMU Smart Grid simulation..."
mvn clean install

# Execute Edge simulation program
echo "Executing Edge Smart Grid simulation..."
mvn exec:java@run-edge

echo "Edge Smart Grid simulation completed!" 
echo "Check results in UpfOnEdge_PdcOnEdge/output/ directory" 