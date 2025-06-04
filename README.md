# PMU Smart Grid Simulation System

## 🔋 Overview

This system simulates a **Smart Grid Power Monitoring Network** using **PMU (Phasor Measurement Unit) sensors** that send data to a **TSO (Transmission System Operator)** for real-time grid analysis. The system models realistic network delays, distance-based routing, and dynamic data collection windows.

### 🎯 Core Concept

**PMU Sensors → GNB/Edge Datacenters → TELCO Hub → TSO Cloud**

1. **Multiple PMU sensors** send **synchronized measurements** periodically
2. **Distance-based routing** selects the closest GNB for each PMU
3. **Dynamic collection windows** gather PMU data with configurable waiting latency
4. **Grid Analysis tasks** are created for centralized power grid monitoring
5. **Realistic network simulation** with hop-by-hop delays and distances

---

## 🏗️ System Architecture

### 📊 Data Flow
```
PMU Sensors (multiple) → Measurements → Network Transfer → Data Collection → Grid Analysis → Results
```

### 🌐 Network Topology
```
PMU Sensors → GNB (Edge) → TELCO (Hub) → TSO (Cloud)
    ↑               ↑            ↑           ↑
    5G        MAN Network   MAN Network   WAN Network
 
```

---

## 🔧 Core Components

### 1. **PmuSimulation.java** - Main Entry Point
**Responsibility**: Starts the simulation and coordinates all components
- Loads configuration from XML files
- Sets up custom components (NetworkModel, TaskGenerator, etc.)
- Executes post-simulation analysis

### 2. **PmuNetworkModel.java** - Realistic Network Simulation
**Responsibility**: Calculates realistic network delays with distance-based routing
- **Distance-based GNB selection**: Finds closest GNB for each PMU
- **Hop-by-hop calculations**: PMU→GNB→TELCO→TSO with realistic timing
- **Distance delays**: Configurable physical signal propagation
- **Network jitter**: Gaussian noise for realistic variation

### 3. **PmuDataCollectorDynamic.java** - Smart Data Collection
**Responsibility**: Collects PMU data in dynamic windows and creates Grid Analysis Task
- **Generation time grouping**: Tasks with same generation time together
- **Configurable waiting latency**: Timeout for late arrivals
- **Deadline enforcement**: DEADLINE_MISSED for late PMU data
- **Grid Analysis creation**: Analysis tasks when window closes

### 4. **PmuTaskOrchestrator.java** - Task Management
**Responsibility**: Orchestrates where PMU sends data, Separates PMU data tasks from Grid Analysis tasks
- **Loop prevention**: Prevents infinite loops from completed Grid Analysis
- **Event forwarding**: Sends PMU data to DataCollector

### 5. **PmuTaskGenerator.java** - Synchronized PMU Data
**Responsibility**: Creates synchronized measurements from all PMUs
- **Perfect synchronization**: All PMUs send data at the same time
- **Variable data size**: Configurable data size with variation

### 6. **PmuSimulationManager.java** - Simulation Coordination
**Responsibility**: Coordinates all simulation components
- **Component integration**: Sets up NetworkModel, DataCollector, Logger
- **Event routing**: Forwards PMU_DATA_RECEIVED events to DataCollector
- **Statistics collection**: Gathers final statistics

### 7. **PmuLogger.java** - Comprehensive Logging
**Responsibility**: Records all system activities
- **Dual CSV outputs**: PMU data transfers + Grid Analysis tasks
- **Deadline tracking**: DEADLINE_MISSED flag for late arrivals
- **Hop-by-hop details**: Network path with times and distances
- **Statistics generation**: Produces comprehensive analysis data

### 8. **PmuLogAnalysis.py** - Post-Simulation Analysis
**Responsibility**: Analyzes simulation results and creates visualizations
- **Network analysis**: Hop times, distances, deadline misses
- **Simulation map**: Visual representation of PMU network
- **Statistics generation**: Comprehensive performance metrics

---

## 📈 Simulation Phases

### Phase 1: **Initialization**
1. **Configuration loading**: XML files for PMUs, GNBs, TELCO, TSO
2. **Component setup**: Network model, task generator, data collector
3. **Logger initialization**: CSV headers and output directories

### Phase 2: **PMU Data Generation**
1. **Synchronized measurements**: All PMUs send data 
2. **Network transfer**: Distance-based routing with realistic delays
3. **Data collection**: Dynamic windows group measurements by generation time

### Phase 3: **Grid Analysis**
1. **Window completion**: When PMU data collection window closes
2. **Analysis task creation**: Grid Analysis tasks for power monitoring is created and executed


### Phase 4: **Results & Analysis**
1. **Log generation**: Detailed CSV files with network and task data
2. **Visualization**: Maps and charts from Python analysis
3. **Statistics**: Performance metrics and system analysis

---

## 🎛️ Key Parameters

### Network Configuration
- **Distance delays**: Configurable physical propagation speed
- **PMU data size**: Configurable per measurement with variation
- **Collection window**: Configurable max waiting time
- **Network jitter**: Gaussian variation for realism

### Smart Grid Specifics
- **PMU frequency**: Configurable measurements per time unit
- **Grid Analysis**: Configurable computational complexity
- **Analysis latency**: Configurable max processing time


---

## 📁 Output Files

### CSV Data Files
- **`Sequential_simulation_pmu.csv`**: PMU data transfers with hop details
- **`Sequential_simulation_state_estimation.csv`**: Grid Analysis task results

### Analysis Results
- **`pmu_simulation_map.png`**: Network topology visualization
- **`pmu_simulation_statistics.txt`**: Comprehensive performance metrics

### Log Files
- **`pmu_simulation.log`**: Detailed execution timeline

---

## 🔬 Technical Details for Extensions

### Custom Network Model
**PmuNetworkModel** extends **DefaultNetworkModel** for:
- **NetworkTransferResult**: Container for hop times and distances
- **Distance calculations**: Euclidean distance with location coordinates
- **GNB selection algorithm**: Closest edge datacenter with coverage check
- **Hop timing calculation**: Bandwidth + latency + distance delays

### Data Collection Algorithm
**PmuDataCollectorDynamic** implements:
- **Generation time buffering**: Tasks grouped by `task.getTime()`
- **Timeout mechanism**: `GENERATION_TIME_TIMEOUT` events for window closing
- **Deadline enforcement**: Drop tasks arriving after `MAX_WAITING_LATENCY`
- **PDC waiting time**: Accurate calculation for State Estimation analysis

### Event-Driven Architecture
- **PMU_DATA_RECEIVED**: PMU data arrival at TSO
- **GENERATION_TIME_TIMEOUT**: Collection window timeout
- **Task routing**: Orchestrator forwards events based on task type


--

## 🚀 Quick Start

See [QUICK_START.md](QUICK_START.md) for step-by-step execution instructions.

---
