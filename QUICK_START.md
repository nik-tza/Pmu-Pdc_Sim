# PMU Smart Grid Simulation - Quick Start Guide

## 🚀 Running the Three Scenarios

The PMU Smart Grid simulation system implements **3 different deployment scenarios** that can be executed using dedicated scripts.

### **Scenario 1: UpfOnTelco_PdcOnCloud** (Cloud Processing)
```bash
# Execute Scenario 1 - TSO Cloud Processing
chmod +x s1_sim.sh && ./s1_sim.sh
```
- **UPF Location**: TELCO Hub  
- **PDC Location**: TSO Cloud
- **Data Flow**: PMU → GNB → TELCO → TSO (PDC processing) → Results
- **Characteristics**: Centralized processing, highest latency, cloud scalability

### **Scenario 2: UpfOnTelco_PdcOnEdge** (Hybrid Processing)
```bash
# Execute Scenario 2 - Edge Processing via TELCO routing
chmod +x s2_sim.sh && ./s2_sim.sh
```
- **UPF Location**: TELCO Hub
- **PDC Location**: Edge Datacenters (GNBs)
- **Data Flow**: PMU → GNB → TELCO → back to GNB (PDC processing) → Results
- **Characteristics**: Hybrid approach, GNB processing with TELCO routing

### **Scenario 3: UpfOnEdge_PdcOnEdge** (Edge Processing)
```bash
# Execute Scenario 3 - Direct Edge Processing
chmod +x s3_sim.sh && ./s3_sim.sh
```
- **UPF Location**: Edge Datacenters (GNBs)
- **PDC Location**: Edge Datacenters (GNBs) 
- **Data Flow**: PMU → GNB (PDC processing) → Results
- **Characteristics**: Lowest latency, distributed processing

---

## 📊 Simulation Results

Each scenario generates results in its own folder:

### **Scenario 1 Output**: `UpfOnTelco_PdcOnCloud/output/`
- **`cloud_simulation_map.png`**: Network topology with TSO processing
- **`Sequential_simulation_pmu.csv`**: PMU data transfers (3-hop: PMU→GNB→TELCO→TSO)
- **`Sequential_simulation_state_estimation.csv`**: TSO processing results
- **`performance_analysis_charts.png`**: Performance metrics
- **`network_bandwidth_usage_charts.png`**: Cloud network utilization
- **`pmu_simulation_statistics.txt`**: Comprehensive statistics

### **Scenario 2 Output**: `UpfOnTelco_PdcOnEdge/output/`
- **`telco_simulation_map.png`**: Network topology with hybrid routing
- **`Sequential_simulation_pmu.csv`**: PMU data transfers (3-hop: PMU→GNB→TELCO→GNB)
- **`Sequential_simulation_state_estimation.csv`**: Edge processing results
- **`performance_analysis_charts.png`**: Performance metrics
- **`network_bandwidth_usage_charts.png`**: Hybrid network utilization
- **`pmu_simulation_statistics.txt`**: Comprehensive statistics

### **Scenario 3 Output**: `UpfOnEdge_PdcOnEdge/output/`
- **`edge_simulation_map.png`**: Network topology with direct edge processing
- **`Sequential_simulation_pmu.csv`**: PMU data transfers (1-hop: PMU→GNB)
- **`Sequential_simulation_state_estimation.csv`**: Edge processing results
- **`performance_analysis_charts.png`**: Performance metrics
- **`network_bandwidth_usage_charts.png`**: Edge network utilization
- **`pmu_simulation_statistics.txt`**: Comprehensive statistics

---

## ⚙️ Manual Execution (Alternative)

If you prefer to run simulations manually:

### Step 1: Compilation
```bash
mvn clean compile
```

### Step 2: Execution by Scenario
```bash
# Scenario 1 - Cloud Processing
mvn exec:java -Dexec.mainClass="UpfOnTelco_PdcOnCloud.CloudSimulation"

# Scenario 2 - Hybrid Processing  
mvn exec:java -Dexec.mainClass="UpfOnTelco_PdcOnEdge.TelcoSimulation"

# Scenario 3 - Edge Processing
mvn exec:java -Dexec.mainClass="UpfOnEdge_PdcOnEdge.EdgeSimulation"
```

### Step 3: Manual Analysis (if needed)
```bash
# Scenario 1 Analysis
cd UpfOnTelco_PdcOnCloud
python3 CloudLogAnalysis.py output

# Scenario 2 Analysis
cd UpfOnTelco_PdcOnEdge  
python3 TelcoLogAnalysis.py output

# Scenario 3 Analysis
cd UpfOnEdge_PdcOnEdge
python3 EdgeLogAnalysis.py output
```

---

## 🔧 Configuration

### Core Configuration Files (shared by all scenarios):
```
settings/
├── simulation_parameters.properties    # Basic parameters
├── applications.xml                   # PMU app configuration
├── edge_datacenters.xml              # GNB + TELCO + TSO positions
├── devices_list.xml                  # PMU sensor positions
└── cloud_datacenters.xml             # TSO cloud specifications
```

### Scenario-Specific Settings:
Each scenario has its own settings folder:
- **`UpfOnTelco_PdcOnCloud/settings/`**: Cloud scenario configuration
- **`UpfOnTelco_PdcOnEdge/settings/`**: Hybrid scenario configuration  
- **`UpfOnEdge_PdcOnEdge/settings/`**: Edge scenario configuration

---

## 🔢 Changing PMU Count

### Method: simulation_parameters.properties
```properties
# File: [scenario]/settings/simulation_parameters.properties
min_number_of_edge_devices=30   # Change both to same number
max_number_of_edge_devices=30   # Change both to same number
```

### Network Area (for more PMUs):
```properties
length=3000          # Increase simulation area
width=3000           # Increase simulation area
```

---

## ⏱️ Key Parameters

### PMU Collection Parameters
```properties
# Maximum PDC waiting time for PMU data
max_pdc_waiting_time=0.0449    # seconds

# Simulation duration
simulation_duration=20.0       # minutes
```

### Network Parameters
```properties
# Network latencies
cellular_latency=0.05    # PMU → GNB
man_latency=0.01        # GNB → TELCO  
wan_latency=0.08        # TELCO → TSO

# Network bandwidths (bits per second)
cellular_bandwidth=50000000     
man_bandwidth=1000000000       
wan_bandwidth=100000000        
```

### PMU Measurement Settings
```xml
<!-- File: settings/applications.xml -->
<application>
    <type>PMU_Data</type>
    <rate>1</rate>           <!-- measurements per second -->
    <latency>2.0</latency>   <!-- max Grid Analysis latency -->
</application>
```

---

## 📈 Performance Comparison

Run all three scenarios to compare:

```bash
# Execute all scenarios
./s1_sim.sh  # Cloud scenario
./s2_sim.sh  # Hybrid scenario  
./s3_sim.sh  # Edge scenario

# Compare results:
# - Network latencies: End-to-end transfer times
# - Processing delays: PDC location impact
# - Deadline compliance: On-time delivery rates
# - Resource utilization: Network and computational load
```

Each scenario provides different insights into smart grid deployment trade-offs between **latency**, **processing power**, and **network utilization**.

---

## 🎯 Understanding the Scenarios

### **When to use Scenario 1 (Cloud)**:
- ✅ Need maximum computational power
- ✅ Complex grid analysis algorithms
- ✅ Historical data analysis
- ❌ Real-time latency requirements

### **When to use Scenario 2 (Hybrid)**:
- ✅ Balance between processing power and latency
- ✅ TELCO infrastructure available
- ✅ Moderate real-time requirements
- ✅ Load balancing needs

### **When to use Scenario 3 (Edge)**:
- ✅ Strict real-time requirements
- ✅ Local grid management
- ✅ Minimal network dependency
- ❌ Limited computational resources per GNB
