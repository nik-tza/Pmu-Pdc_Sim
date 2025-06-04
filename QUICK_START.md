# PMU Smart Grid Simulation - Quick Start Guide

## 🚀 Running the Simulation

### **Fast method:** Script **p_run_simulation.sh**
```bash
chmod +x p_run_simulation.sh && ./p_run_simulation.sh  
```

### **Second method:**

### Step 1: Compilation
```bash
mvn clean compile
```

### Step 2: Execution
```bash
# Using the main simulation class
mvn exec:java -Dexec.mainClass="PmuSim.PmuSimulation"
```

### Step 3: Results Analysis
```bash
# The simulation automatically runs the analysis
# Results will be found at:
ls PmuSim/Pmu_output/[timestamp]/
```

---

## ⚙️ Configuration Files

### 📍 Core configuration files:

```
PmuSim/Pmu_settings/
├── simulation_parameters.properties    # Basic parameters
├── applications.xml                   # PMU app configuration
├── edge_datacenters.xml              # GNB + TELCO + TSO positions
├── devices_list.xml                  # PMU sensor positions
└── cloud_datacenters.xml             # TSO cloud specifications
```

---

## 🔢 Changing the Number of PMUs

### Method 1: simulation_parameters.properties
```properties
# File: PmuSim/Pmu_settings/simulation_parameters.properties
min_number_of_edge_devices=30   #change both to same number
max_number_of_edge_devices=30   #change both to same number
```

---

## 🌐 Network Configuration

### Changing Network Parameters
```properties
# File: PmuSim/Pmu_settings/simulation_parameters.properties

# Simulation Area (larger area for more PMUs)
length=3000          # Increase as needed
width=3000           # Increase as needed

# PMU Frequency (measurements per second)
# See applications.xml for details

# Network Latencies
cellular_latency=0.05    # PMU → GNB
man_latency=0.01        # GNB → TELCO  
wan_latency=0.08        # TELCO → TSO

# Network Bandwidths (bits per second)
cellular_bandwidth=50000000     # Configure as needed
man_bandwidth=1000000000       # Configure as needed
wan_bandwidth=100000000        # Configure as needed
```

### Distance Delays (for realistic propagation)
```java
// File: PmuSim/PmuNetworkModel.java (around line 37)
private static final double DISTANCE_DELAY_MICROSECONDS_PER_METER = 30.0; 
```

---

## ⏱️ Collection Parameters

### Max PDC Waiting Time for PMU Data to arive
```java
// File: PmuSim/PmuDataCollectorDynamic.java (around line 27)
private static final double MAX_WAITING_LATENCY = 0.0449; // Configure as needed
```

### PMU Measurement Frequency
```xml
<!-- File: PmuSim/Pmu_settings/applications.xml -->
<application>
    <type>PMU_Data</type>
    <rate>1</rate>           <!-- measurements per second -->
    <latency>2.0</latency>   <!-- max Grid Analysis latency -->
</application>
```

---

## 🏗️ Infrastructure Configuration

### Adding New GNB Edge Datacenters
```xml
<!-- File: PmuSim/Pmu_settings/edge_datacenters.xml -->
<datacenters>
    <!-- Existing GNBs -->
    
    <!-- New GNB -->
    <datacenter>
        <id>5</id>
        <name>EDGE_5</name>
        <location>
            <x_pos>1500</x_pos>
            <y_pos>1500</y_pos>
        </location>
        <periphery>true</periphery>
        <costPerCore>0.01</costPerCore>
        <numberOfCores>8</numberOfCores>
        <mipsPerCore>2000</mipsPerCore>
        <ramGB>16</ramGB>
        <storageGB>500</storageGB>
    </datacenter>
</datacenters>
```

### Network Links between GNBs and TELCO
```xml
<!-- File: PmuSim/Pmu_settings/edge_datacenters.xml -->
<network_links>
    <!-- New connection for the new GNB -->
    <link>
        <from>EDGE_5</from>
        <to>TELCO</to>
        <latency>0.01</latency>
    </link>
</network_links>
```

---

## 📊 Simulation Duration & Scale

### Simulation Time
```properties
# File: PmuSim/Pmu_settings/simulation_parameters.properties
simulation_duration=20.0    # Configure as needed (minutes)
```

### Grid Analysis Complexity
```java
// File: PmuSim/PmuDataCollectorDynamic.java (around line 22)
private static final long GRID_ANALYSIS_LENGTH_MI = 15000; // Configure as needed (how many computing resources the task requires)
```

---

## 🎯 PMU Distribution Patterns

### Uniform Grid Pattern
```xml
<!-- Place PMUs in grid formation -->
<!-- x_pos: 200, 400, 600, 800, 1000, 1200, 1400, 1600, 1800 -->
<!-- y_pos: 200, 400, 600, 800, 1000, 1200, 1400, 1600, 1800 -->
```

### Clustered Pattern (near cities)
```xml
<!-- Group PMUs around specific locations -->
<!-- Cluster 1: (500,500) area -->
<!-- Cluster 2: (1500,1500) area -->
```

### Random Distribution
```python
# Use Python script to generate random coordinates
import random

simulation_area = 2000  # meters
pmu_count = 50

for i in range(pmu_count):
    x = random.randint(100, simulation_area-100)
    y = random.randint(100, simulation_area-100)
    print(f"<device><id>{i}</id><location><x_pos>{x}</x_pos><y_pos>{y}</y_pos></location><applications>PMU_Data</applications></device>")
```

---

## 📈 Output Customization

### CSV Output Fields
To add new fields to CSV files:

```java
// File: PmuSim/PmuLogger.java

// PMU Data CSV Header (around line 39)
private static final String CSV_HEADER = "Time,PmuID,PmuCoordinates,DataSize,Path,HopSum,Status,CustomField";

// Grid Analysis CSV Header (around line 43)  
private static final String STATE_ESTIMATION_CSV_HEADER = "Time,TaskID,Window,Coverage,BatchType,InputDataKB,OutputDataKB,MaxLatency,ComputationMI,WaitTime,ExecTime,NetTime,TotalTime,Status,PDCWaitingTime,SuccessFlag,CustomMetric";
```

### Log Detail Level
```java
// File: PmuSim/PmuLogger.java (around line 28)
private static boolean PRINT_TO_TERMINAL = false; 

// Change to true for verbose output to terminal:
private static boolean PRINT_TO_TERMINAL = true;
```

---
