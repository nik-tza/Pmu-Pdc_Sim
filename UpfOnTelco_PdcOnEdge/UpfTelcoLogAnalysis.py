#!/usr/bin/env python3
"""
Edge Log Analysis Script
Analyzes Edge Smart Grid simulation data and creates visualization maps.
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import os
import sys
import argparse
import xml.etree.ElementTree as ET
import logging
from typing import Dict, List, Tuple, Optional
import re
from datetime import datetime
import csv

# Configuration
BASE_OUTPUT_DIR = "UpfOnTelco_PdcOnEdge/output"
PMU_SETTINGS_DIR = "UpfOnTelco_PdcOnEdge/settings"
SHOW_PLOTS = True

# Output files
SIMULATION_MAP_CHART = "edge_simulation_map.png"

def setup_logging(output_folder: str) -> logging.Logger:
    """Setup logging for the analysis."""
    logger = logging.getLogger('PmuSimulationAnalysis')
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(message)s')
    
    # Console handler only
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(console_handler)
    
    return logger

def read_simulation_parameters() -> Dict[str, any]:
    """Read simulation parameters from properties file."""
    params = {
        'length': 2000,
        'width': 2000,
        'edge_datacenters_coverage': 800,
        'max_edge_devices': 14  # Default fallback
    }
    
    try:
        properties_file = os.path.join(PMU_SETTINGS_DIR, "simulation_parameters.properties")
        with open(properties_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('length='):
                    params['length'] = int(line.split('=')[1])
                elif line.startswith('width='):
                    params['width'] = int(line.split('=')[1])
                elif line.startswith('edge_datacenters_coverage='):
                    params['edge_datacenters_coverage'] = float(line.split('=')[1])
                elif line.startswith('max_number_of_edge_devices='):
                    params['max_edge_devices'] = int(line.split('=')[1])
    except Exception as e:
        print(f"Error reading simulation parameters: {e}")
    
    return params

def parse_edge_datacenters_xml() -> List[Dict[str, any]]:
    """Parse edge datacenters from XML file."""
    datacenters = []
    
    try:
        xml_file = os.path.join(PMU_SETTINGS_DIR, "edge_datacenters.xml")
        tree = ET.parse(xml_file)
        root = tree.getroot()
        
        for datacenter in root.findall('datacenter'):
            name = datacenter.get('name')
            location = datacenter.find('location')
            x_pos = float(location.find('x_pos').text)
            y_pos = float(location.find('y_pos').text)
            periphery = datacenter.find('periphery').text.lower() == 'true'
            
            datacenters.append({
                'name': name,
                'x': x_pos,
                'y': y_pos,
                'periphery': periphery
            })
            
    except Exception as e:
        print(f"Error parsing edge datacenters XML: {e}")
    
    return datacenters

def parse_network_links_xml() -> List[Dict[str, any]]:
    """Parse network links from XML file."""
    links = []
    
    try:
        xml_file = os.path.join(PMU_SETTINGS_DIR, "edge_datacenters.xml")
        tree = ET.parse(xml_file)
        root = tree.getroot()
        
        network_links = root.find('network_links')
        if network_links is not None:
            for link in network_links.findall('link'):
                from_node = link.find('from').text
                to_node = link.find('to').text
                latency_elem = link.find('latency')
                latency = float(latency_elem.text) if latency_elem is not None else 0.0
                
                links.append({
                    'from': from_node,
                    'to': to_node,
                    'latency': latency
                })
                
    except Exception as e:
        print(f"Error parsing network links XML: {e}")
    
    return links

def read_pmu_positions_from_csv(simulation_folder: str) -> Tuple[List[Dict[str, any]], Dict[str, Dict[str, float]]]:
    """Read PMU positions from Sequential_simulation_pmu.csv file and calculate hop averages."""
    pmus = []
    seen_pmu_ids = set()
    
    # Dictionary to store hop times for each PMU-GNB connection and calculate averages
    hop_times = {}  # {pmu_id: {'gnb': times_list, 'gnb_to_telco': times_list, 'telco_to_tso': times_list}}
    hop_averages = {}  # {pmu_id: {'gnb': avg_time, 'gnb_to_telco': avg_time, 'telco_to_tso': avg_time}}
    
    # **NEW: Track DEADLINE_MISSED data per PMU**
    deadline_missed_stats = {}  # {pmu_id: {'total_transfers': count, 'deadline_missed': count}}
    
    try:
        # **FIXED: Only look for Sequential_simulation_pmu_data_transfers.csv**
        pmu_csv_file = os.path.join(simulation_folder, "Sequential_simulation_pmu_data_transfers.csv")
        
        if not os.path.exists(pmu_csv_file):
            print(f"ERROR: Sequential_simulation_pmu_data_transfers.csv not found at: {pmu_csv_file}")
            print("Available files in simulation folder:")
            try:
                for file in os.listdir(simulation_folder):
                    print(f"  - {file}")
            except Exception as e:
                print(f"Could not list directory: {e}")
            return [], {}
        
        print(f"✓ Found PMU CSV file: {pmu_csv_file}")
        
        # **FIXED: Read CSV with proper CSV parsing using csv module**
        print("📖 Loading CSV file with proper CSV parsing...")
        
        # **Read the CSV file using csv module**
        with open(pmu_csv_file, 'r') as file:
            csv_reader = csv.reader(file)
            
            # Read header
            header = next(csv_reader)
            print(f"✓ CSV header: {header}")
            
            # **Verify we have the expected 7 columns**
            if len(header) != 7:
                print(f"❌ ERROR: Expected 7 columns in header, got {len(header)}")
                return [], {}
            
            # **Debug: Track status values**
            status_counts = {}
            debug_entries = 0
            
            expected_columns = ['Time', 'PmuID', 'PmuCoordinates', 'DataSize', 'Path', 'HopSum', 'Status']
            for i, col in enumerate(expected_columns):
                if i < len(header) and header[i] != col:
                    print(f"⚠️  WARNING: Expected column '{col}' at position {i}, got '{header[i]}'")
            
            # **Process data rows**
            processed_entries = 0
            processed_pmus = 0
            
            for line_num, row in enumerate(csv_reader, start=2):  # Start from line 2 (after header)
                try:
                    # **Ensure we have exactly 7 columns**
                    if len(row) != 7:
                        print(f"❌ Skipping line {line_num}: Got {len(row)} columns, need exactly 7")
                        continue
                    
                    # **Extract all 7 fields**
                    time_val = float(row[0])
                    pmu_id = int(row[1])
                    pmu_coordinates = row[2].strip('"')  # Remove quotes if present
                    data_size = float(row[3])
                    path = row[4].strip('"')  # Remove quotes if present  
                    hop_sum = float(row[5])
                    status = row[6].strip()  # Status column (OK or DEADLINE_MISSED)
                    
                    # **Check for DEADLINE_MISSED in the Status column ('L' = Deadline Missed, 'S' = Success)**
                    deadline_missed = (status == 'L')
                    
                    # **Debug: Track status values for first few entries**
                    debug_entries += 1
                    if status not in status_counts:
                        status_counts[status] = 0
                    status_counts[status] += 1
                    
                    if debug_entries <= 10:
                        print(f"🔍 Entry {debug_entries}: Status = '{status}', deadline_missed = {deadline_missed}")
                    
                    processed_entries += 1
                    
                    # **Track deadline missed statistics per PMU**
                    if pmu_id not in deadline_missed_stats:
                        deadline_missed_stats[pmu_id] = {'total_transfers': 0, 'deadline_missed': 0}
                    
                    deadline_missed_stats[pmu_id]['total_transfers'] += 1
                    if deadline_missed:
                        deadline_missed_stats[pmu_id]['deadline_missed'] += 1
                    
                    # **Parse Path to extract GNB and hop times WITH DISTANCES**
                    if '->' in path:
                        # Parse NEW format: "PMU -> GNB_1 (0.0060s, 42.9m) -> TELCO (0.0114s, 500.0m) -> TSO (0.0606s, 1415.6m)"
                        path_parts = path.split(' -> ')
                        
                        gnb_name = None
                        pmu_to_gnb_time = None
                        pmu_to_gnb_distance = None
                        gnb_to_telco_time = None
                        gnb_to_telco_distance = None
                        telco_to_tso_time = None
                        telco_to_tso_distance = None
                        
                        # **Extract GNB name, PMU->GNB time and distance**
                        if len(path_parts) >= 2:
                            gnb_part = path_parts[1].strip()  # "GNB_1 (0.0060s, 42.9m)"
                            # Updated regex to capture both time and distance
                            gnb_match = re.match(r'(GNB_\d+|GNB_Unknown)\s*\(([\d.]+)s,\s*([\d.]+)m\)', gnb_part)
                            if gnb_match:
                                gnb_name = gnb_match.group(1)
                                pmu_to_gnb_time = float(gnb_match.group(2))
                                pmu_to_gnb_distance = float(gnb_match.group(3))
                        
                        # **Extract GNB->TELCO time and distance**
                        if len(path_parts) >= 3:
                            telco_part = path_parts[2].strip()  # "TELCO (0.0114s, 500.0m)"
                            telco_match = re.search(r'\(([\d.]+)s,\s*([\d.]+)m\)', telco_part)
                            if telco_match:
                                gnb_to_telco_time = float(telco_match.group(1))
                                gnb_to_telco_distance = float(telco_match.group(2))
                        
                        # **Extract TELCO->TSO time and distance**
                        if len(path_parts) >= 4:
                            tso_part = path_parts[3].strip()  # "TSO (0.0606s, 1415.6m)"
                            tso_match = re.search(r'\(([\d.]+)s,\s*([\d.]+)m\)', tso_part)
                            if tso_match:
                                telco_to_tso_time = float(tso_match.group(1))
                                telco_to_tso_distance = float(tso_match.group(2))
                        
                        # **Store hop times and distances for averaging**
                        if pmu_id not in hop_times:
                            hop_times[pmu_id] = {
                                'gnb_times': [], 'gnb_to_telco_times': [], 'telco_to_tso_times': [],
                                'gnb_distances': [], 'gnb_to_telco_distances': [], 'telco_to_tso_distances': [],
                                'gnb_name': gnb_name
                            }
                        
                        if pmu_to_gnb_time is not None:
                            hop_times[pmu_id]['gnb_times'].append(pmu_to_gnb_time)
                        if pmu_to_gnb_distance is not None:
                            hop_times[pmu_id]['gnb_distances'].append(pmu_to_gnb_distance)
                        if gnb_to_telco_time is not None:
                            hop_times[pmu_id]['gnb_to_telco_times'].append(gnb_to_telco_time)
                        if gnb_to_telco_distance is not None:
                            hop_times[pmu_id]['gnb_to_telco_distances'].append(gnb_to_telco_distance)
                        if telco_to_tso_time is not None:
                            hop_times[pmu_id]['telco_to_tso_times'].append(telco_to_tso_time)
                        if telco_to_tso_distance is not None:
                            hop_times[pmu_id]['telco_to_tso_distances'].append(telco_to_tso_distance)
                        if gnb_name:
                            hop_times[pmu_id]['gnb_name'] = gnb_name
                    
                    # **If we haven't seen this PMU ID before, store its position**
                    if pmu_id not in seen_pmu_ids:
                        # **Parse coordinates from string format "(x,y)"**
                        if processed_pmus < 5:
                            print(f"🎯 Processing PMU {pmu_id}: coords_str = '{pmu_coordinates}', status = '{status}', deadline_missed = {deadline_missed}")
                        
                        # **Parse coordinates from string format "(x,y)"**
                        if pmu_coordinates.startswith('(') and pmu_coordinates.endswith(')'):
                            coords_clean = pmu_coordinates[1:-1]  # Remove parentheses
                            try:
                                if ',' in coords_clean:
                                    x_str, y_str = coords_clean.split(',', 1)  # Split on first comma only
                                    pmu_x = float(x_str.strip())
                                    pmu_y = float(y_str.strip())
                                    
                                    # **Store this PMU with the actual PMU ID from PmuID**
                                    pmus.append({
                                        'id': pmu_id,
                                        'x': pmu_x,
                                        'y': pmu_y
                                    })
                                    seen_pmu_ids.add(pmu_id)
                                    processed_pmus += 1
                                    
                                    if processed_pmus <= 5:  # Show first 5 PMUs for debugging
                                        print(f"✅ PMU {pmu_id} at position ({pmu_x:.1f}, {pmu_y:.1f})")
                                else:
                                    if processed_pmus < 5:
                                        print(f"❌ No comma found in coords: '{coords_clean}'")
                            except ValueError as e:
                                if processed_pmus < 5:
                                    print(f"❌ Error parsing coordinates '{pmu_coordinates}' for PMU {pmu_id}: {e}")
                        else:
                            if processed_pmus < 5:
                                print(f"❌ Invalid coordinate format '{pmu_coordinates}' for PMU {pmu_id}")
                    
                except (ValueError, IndexError) as e:
                    if processed_entries <= 5:
                        print(f"❌ Error processing line {line_num}: {e}")
                    continue
                
                # Continue processing all entries in the CSV file for accurate statistics
            
            print(f"✅ Processed {processed_entries} entries: {processed_pmus} unique PMUs found")
            
            # **Debug: Print status value counts**
            print(f"📊 Status value counts: {status_counts}")
            total_deadline_missed = status_counts.get('L', 0)
            total_success = status_counts.get('S', 0)
            print(f"📈 Total deadline missed (L): {total_deadline_missed}, Total success (S): {total_success}")
            
            # **Store deadline missed stats in hop_averages for later use**
            global_deadline_missed_stats = deadline_missed_stats
            
    except Exception as e:
        print(f"❌ Error in read_pmu_positions_from_csv: {e}")
        import traceback
        traceback.print_exc()
        return [], {}
    
    # **Calculate averages for each PMU**
    print("📊 Calculating hop time and distance averages...")
    for pmu_id, times_data in hop_times.items():
        hop_averages[pmu_id] = {}
        
        # **Calculate average PMU->GNB time and distance**
        if times_data['gnb_times']:
            hop_averages[pmu_id]['gnb'] = sum(times_data['gnb_times']) / len(times_data['gnb_times'])
            hop_averages[pmu_id]['gnb_count'] = len(times_data['gnb_times'])
        else:
            hop_averages[pmu_id]['gnb'] = 0.0
            hop_averages[pmu_id]['gnb_count'] = 0
        
        if times_data['gnb_distances']:
            hop_averages[pmu_id]['gnb_distance'] = sum(times_data['gnb_distances']) / len(times_data['gnb_distances'])
        else:
            hop_averages[pmu_id]['gnb_distance'] = 0.0
        
        # **Calculate average GNB->TELCO time and distance**
        if times_data['gnb_to_telco_times']:
            hop_averages[pmu_id]['gnb_to_telco'] = sum(times_data['gnb_to_telco_times']) / len(times_data['gnb_to_telco_times'])
            hop_averages[pmu_id]['gnb_to_telco_count'] = len(times_data['gnb_to_telco_times'])
        else:
            hop_averages[pmu_id]['gnb_to_telco'] = 0.0
            hop_averages[pmu_id]['gnb_to_telco_count'] = 0
        
        if times_data['gnb_to_telco_distances']:
            hop_averages[pmu_id]['gnb_to_telco_distance'] = sum(times_data['gnb_to_telco_distances']) / len(times_data['gnb_to_telco_distances'])
        else:
            hop_averages[pmu_id]['gnb_to_telco_distance'] = 0.0
        
        # **Calculate average TELCO->TSO time and distance**  
        if times_data['telco_to_tso_times']:
            hop_averages[pmu_id]['telco_to_tso'] = sum(times_data['telco_to_tso_times']) / len(times_data['telco_to_tso_times'])
            hop_averages[pmu_id]['telco_to_tso_count'] = len(times_data['telco_to_tso_times'])
        else:
            hop_averages[pmu_id]['telco_to_tso'] = 0.0
            hop_averages[pmu_id]['telco_to_tso_count'] = 0
        
        if times_data['telco_to_tso_distances']:
            hop_averages[pmu_id]['telco_to_tso_distance'] = sum(times_data['telco_to_tso_distances']) / len(times_data['telco_to_tso_distances'])
        else:
            hop_averages[pmu_id]['telco_to_tso_distance'] = 0.0
        
        # **Store GNB name**
        hop_averages[pmu_id]['gnb_name'] = times_data.get('gnb_name', 'GNB_Unknown')
        
        # **Store deadline missed stats for this PMU**
        if pmu_id in deadline_missed_stats:
            hop_averages[pmu_id]['total_transfers'] = deadline_missed_stats[pmu_id]['total_transfers']
            hop_averages[pmu_id]['deadline_missed'] = deadline_missed_stats[pmu_id]['deadline_missed']
            hop_averages[pmu_id]['deadline_missed_rate'] = (deadline_missed_stats[pmu_id]['deadline_missed'] / deadline_missed_stats[pmu_id]['total_transfers']) * 100 if deadline_missed_stats[pmu_id]['total_transfers'] > 0 else 0
        else:
            hop_averages[pmu_id]['total_transfers'] = 0
            hop_averages[pmu_id]['deadline_missed'] = 0
            hop_averages[pmu_id]['deadline_missed_rate'] = 0
        
        if pmu_id <= 5:  # Show first 5 PMUs for debugging
            print(f"📈 PMU {pmu_id} -> {hop_averages[pmu_id]['gnb_name']}: avg {hop_averages[pmu_id]['gnb']:.4f}s ({hop_averages[pmu_id]['gnb_distance']:.1f}m), {hop_averages[pmu_id]['deadline_missed']} missed deadlines")
    
    print(f"🎯 Final Results: {len(pmus)} unique PMUs found, {len(hop_averages)} PMUs with hop averages")
    
    # **DEBUG: Show a few PMU positions to verify they are valid**
    if pmus:
        print("📍 Sample PMU positions:")
        for i, pmu in enumerate(pmus[:5]):
            print(f"  PMU {pmu['id']}: ({pmu['x']:.1f}, {pmu['y']:.1f})")
    
    return sorted(pmus, key=lambda x: x['id']), hop_averages

def calculate_pmu_to_gnb_average_times(simulation_folder: str) -> Dict[int, float]:
    """Calculate average PMU to GNB transfer times from Sequential_simulation_pmu.csv."""
    pmu_to_gnb_times = {}  # {pmu_id: [list of times]}
    
    try:
        pmu_csv_file = os.path.join(simulation_folder, "Sequential_simulation_pmu_data_transfers.csv")
        
        if not os.path.exists(pmu_csv_file):
            print(f"WARNING: Sequential_simulation_pmu_data_transfers.csv not found for PMU-GNB time calculation")
            return {}
        
        print("📊 Calculating PMU to GNB average transfer times...")
        
        with open(pmu_csv_file, 'r') as file:
            csv_reader = csv.reader(file)
            
            # Skip header
            next(csv_reader)
            
            for line_num, row in enumerate(csv_reader, start=2):
                try:
                    if len(row) != 7:
                        continue
                    
                    pmu_id = int(row[1])
                    path = row[4].strip('"')  # Path column
                    
                    # Extract PMU to GNB time from path
                    # Format: "PMU -> GNB_1 (0.0060s, 42.9m) -> TELCO (0.0114s, 500.0m) -> TSO (0.0606s, 1415.6m)"
                    if '->' in path:
                        path_parts = path.split(' -> ')
                        
                        if len(path_parts) >= 2:
                            gnb_part = path_parts[1].strip()  # "GNB_1 (0.0060s, 42.9m)"
                            # Extract time from first parenthesis
                            import re
                            time_match = re.search(r'\(([\d.]+)s,', gnb_part)
                            if time_match:
                                pmu_to_gnb_time = float(time_match.group(1))
                                
                                if pmu_id not in pmu_to_gnb_times:
                                    pmu_to_gnb_times[pmu_id] = []
                                
                                pmu_to_gnb_times[pmu_id].append(pmu_to_gnb_time)
                
                except (ValueError, IndexError) as e:
                    continue
                
                # Continue processing all entries for accurate statistics
        
        # Calculate averages
        pmu_averages = {}
        for pmu_id, times in pmu_to_gnb_times.items():
            if times:
                avg_time = sum(times) / len(times)
                pmu_averages[pmu_id] = avg_time
                if pmu_id <= 5:  # Debug first 5 PMUs
                    print(f"  PMU {pmu_id}: {len(times)} samples, avg time to GNB: {avg_time:.4f}s")
        
        print(f"✅ Calculated average PMU-to-GNB times for {len(pmu_averages)} PMUs")
        return pmu_averages
        
    except Exception as e:
        print(f"❌ Error calculating PMU to GNB times: {e}")
        return {}

def calculate_gnb_to_telco_average_times(simulation_folder: str) -> Dict[str, float]:
    """Calculate average GNB to TELCO transfer times from Sequential_simulation_pmu_data_transfers.csv."""
    gnb_to_telco_times = {}  # {gnb_name: [list of times]}
    
    try:
        pmu_csv_file = os.path.join(simulation_folder, "Sequential_simulation_pmu_data_transfers.csv")
        
        if not os.path.exists(pmu_csv_file):
            print(f"WARNING: Sequential_simulation_pmu_data_transfers.csv not found for GNB-TELCO time calculation")
            return {}
        
        print("📊 Calculating GNB to TELCO average transfer times...")
        
        with open(pmu_csv_file, 'r') as file:
            csv_reader = csv.reader(file)
            
            # Skip header
            next(csv_reader)
            
            for line_num, row in enumerate(csv_reader, start=2):
                try:
                    if len(row) != 7:
                        continue
                    
                    path = row[4].strip('"')  # Path column
                    
                    # Extract GNB to TELCO time from path
                    # Format: "PMU -> GNB_6 (0.0000s, 419.6m) -> TELCO (0.0131s, 707.1m) -> GNB_6 (0.0113s, 707.1m)"
                    if '->' in path:
                        path_parts = path.split(' -> ')
                        
                        # Extract GNB name from first hop
                        gnb_name = None
                        if len(path_parts) >= 2:
                            gnb_part = path_parts[1].strip()  # "GNB_6 (0.0000s, 419.6m)"
                            import re
                            gnb_match = re.match(r'(GNB_\d+)', gnb_part)
                            if gnb_match:
                                gnb_name = gnb_match.group(1)
                        
                        # Extract TELCO time from second hop
                        if len(path_parts) >= 3 and gnb_name:
                            telco_part = path_parts[2].strip()  # "TELCO (0.0131s, 707.1m)"
                            telco_match = re.search(r'\(([\d.]+)s,', telco_part)
                            if telco_match:
                                gnb_to_telco_time = float(telco_match.group(1))
                                
                                if gnb_name not in gnb_to_telco_times:
                                    gnb_to_telco_times[gnb_name] = []
                                
                                gnb_to_telco_times[gnb_name].append(gnb_to_telco_time)
                
                except (ValueError, IndexError) as e:
                    continue
                
                # Continue processing all entries for accurate statistics
        
        # Calculate averages
        gnb_averages = {}
        for gnb_name, times in gnb_to_telco_times.items():
            if times:
                avg_time = sum(times) / len(times)
                gnb_averages[gnb_name] = avg_time
                print(f"  {gnb_name}: {len(times)} samples, avg time to TELCO: {avg_time:.4f}s")
        
        print(f"✅ Calculated average GNB-to-TELCO times for {len(gnb_averages)} GNBs")
        return gnb_averages
        
    except Exception as e:
        print(f"❌ Error calculating GNB to TELCO times: {e}")
        return {}

def create_simulation_map(simulation_folder: str, logger: logging.Logger):
    """Create the PMU simulation map showing GNBs, PMUs, TELCO and connections."""
    if not SHOW_PLOTS:
        return
    
    logger.info("Creating PMU simulation map...")
    
    # Read configuration data
    params = read_simulation_parameters()
    datacenters = parse_edge_datacenters_xml()
    links = parse_network_links_xml()
    pmus, hop_averages = read_pmu_positions_from_csv(simulation_folder)
    
    # **NEW: Get GNB to TELCO average latency times**
    gnb_to_telco_times = calculate_gnb_to_telco_average_times(simulation_folder)
    
    # Get PMU count for map title
    max_pmus = params['max_edge_devices']
    
    # Create the plot
    plt.figure(figsize=(16, 16))
    ax = plt.gca()
    
    # Constants for visualization
    COVERAGE_RADIUS = params['edge_datacenters_coverage']
    DATACENTER_RADIUS = 60.0  # Increased by 4x (from 15.0)
    PMU_SIZE = 25.0
    TSO_SIZE = 120.0  # Increased to be twice the size of EDGE datacenters
    TELCO_SIZE = 140.0  # Increased to be slightly larger than TSO
    
    # Colors
    colors = plt.cm.Set3(np.linspace(0, 1, len(datacenters)))
    
    # Plot edge datacenters (GNBs and TELCO)
    datacenter_positions = {}
    for i, datacenter in enumerate(datacenters):
        x, y = datacenter['x'], datacenter['y']
        name = datacenter['name']
        datacenter_positions[name] = (x, y)
        
        # All datacenters are now GNB (EDGE) - no TELCO
        color = colors[i]
        # Regular circle for all GNB datacenters
        datacenter_circle = plt.Circle((x, y), DATACENTER_RADIUS, 
                                     color=color, 
                                     alpha=0.8,
                                     ec='black',
                                     linewidth=2)
        ax.add_patch(datacenter_circle)
        
        # Plot coverage area (only for peripheral edge datacenters)
        if datacenter['periphery']:
            coverage = plt.Circle((x, y), COVERAGE_RADIUS, 
                                color=color, alpha=0.15, linestyle='--')
            ax.add_patch(coverage)
        
        # Add label
        plt.annotate(name, (x, y), xytext=(5, 5), textcoords='offset points',
                   fontweight='bold', fontsize=12, 
                   bbox=dict(boxstyle="round,pad=0.3", facecolor='white', alpha=0.8))
    
    # **NEW: Add TELCO datacenter in the center as a hexagon (same as CloudLogAnalysis.py)**
    telco_x = params['length'] / 2
    telco_y = params['width'] / 2
    datacenter_positions['TELCO'] = (telco_x, telco_y)
    
    # Create hexagon for TELCO (same style as CloudLogAnalysis.py)
    from matplotlib.patches import Polygon
    hexagon = Polygon([
        (telco_x - TELCO_SIZE, telco_y),
        (telco_x - TELCO_SIZE/2, telco_y + TELCO_SIZE*0.866),
        (telco_x + TELCO_SIZE/2, telco_y + TELCO_SIZE*0.866),
        (telco_x + TELCO_SIZE, telco_y),
        (telco_x + TELCO_SIZE/2, telco_y - TELCO_SIZE*0.866),
        (telco_x - TELCO_SIZE/2, telco_y - TELCO_SIZE*0.866)
    ], color='lightcoral', alpha=0.9, ec='black', linewidth=2)
    ax.add_patch(hexagon)
    
    # Add TELCO label
    plt.annotate('TELCO', (telco_x, telco_y), xytext=(5, 5), textcoords='offset points',
               fontweight='bold', fontsize=12, 
               bbox=dict(boxstyle="round,pad=0.3", facecolor='white', alpha=0.8))
    
    # TSO removed - distributed architecture
    
    # **NEW: Create a mapping from GNB names to Edge datacenter positions**
    gnb_to_edge_mapping = {}
    for datacenter in datacenters:
        if datacenter['name'].startswith('EDGE_'):
            edge_id = datacenter['name'].split('_')[1]
            gnb_name = f"GNB_{edge_id}"
            gnb_to_edge_mapping[gnb_name] = (datacenter['x'], datacenter['y'])
    
    # Plot PMUs as squares and connect to their assigned GNB (not EDGE)
    for pmu in pmus:
        x, y = pmu['x'], pmu['y']
        pmu_id = pmu['id']
        
        # Plot PMU as a square
        pmu_square = plt.Rectangle((x - PMU_SIZE/2, y - PMU_SIZE/2), PMU_SIZE, PMU_SIZE,
                                 color='green', alpha=0.8, ec='darkgreen', linewidth=2)
        ax.add_patch(pmu_square)
        
        # Add PMU label
        plt.annotate(f'PMU_{pmu_id}', (x, y), xytext=(15, 15), textcoords='offset points',
                   fontsize=10, fontweight='bold', color='darkgreen',
                   bbox=dict(boxstyle="round,pad=0.2", facecolor='lightgreen', alpha=0.7))
        
        # **Connect PMU to its assigned GNB with dashed lines and average times + distances**
        if pmu_id in hop_averages:
            gnb_name = hop_averages[pmu_id]['gnb_name']
            avg_time = hop_averages[pmu_id]['gnb']
            avg_distance = hop_averages[pmu_id]['gnb_distance']
            sample_count = hop_averages[pmu_id]['gnb_count']
            
            if gnb_name in gnb_to_edge_mapping:
                gnb_x, gnb_y = gnb_to_edge_mapping[gnb_name]
                
                # Draw dashed connection line between PMU and GNB
                plt.plot([x, gnb_x], [y, gnb_y], 'gray', linestyle='--', 
                        linewidth=2, alpha=0.7)
                
                # **Add average time and distance label on the connection**
                mid_x = (x + gnb_x) / 2
                mid_y = (y + gnb_y) / 2
                plt.annotate(f'{avg_time:.3f}s\n{avg_distance:.0f}m', 
                           (mid_x, mid_y), 
                           xytext=(0, 5), textcoords='offset points',
                           fontsize=7, fontweight='bold', color='blue',
                           ha='center', va='bottom',
                           bbox=dict(boxstyle="round,pad=0.1", facecolor='lightblue', alpha=0.7))
    
    # **NEW: Add connections from all GNB datacenters to TELCO with latency labels**
    for datacenter in datacenters:
        if datacenter['name'].startswith('EDGE_'):
            gnb_x, gnb_y = datacenter['x'], datacenter['y']
            edge_id = datacenter['name'].split('_')[1]
            gnb_name = f"GNB_{edge_id}"
            
            # Draw connection line from GNB to TELCO (same purple color as CloudLogAnalysis.py)
            plt.plot([gnb_x, telco_x], [gnb_y, telco_y], 'purple', linestyle='-', 
                    linewidth=3, alpha=0.7, zorder=1)  # Behind other elements
            
            # **Add latency label on GNB-TELCO connection if available**
            if gnb_name in gnb_to_telco_times:
                avg_latency = gnb_to_telco_times[gnb_name]
                
                # Calculate midpoint for label placement
                mid_x = (gnb_x + telco_x) / 2
                mid_y = (gnb_y + telco_y) / 2
                
                # Add latency label (same purple color as CloudLogAnalysis.py)
                plt.annotate(f'{avg_latency:.4f}s', 
                           (mid_x, mid_y), 
                           xytext=(0, -10), textcoords='offset points',
                           fontsize=8, fontweight='bold', color='purple',
                           ha='center', va='top',
                           bbox=dict(boxstyle="round,pad=0.1", facecolor='plum', alpha=0.7))
    
    # Plot settings
    plt.grid(True, linestyle='--', alpha=0.3)
    plt.xlabel('X Coordinate (meters)', fontsize=12)
    plt.ylabel('Y Coordinate (meters)', fontsize=12)
    plt.title(f'UPF-TELCO Smart Grid Simulation Map\n{max_pmus} PMU Sensors, {len(datacenters)} GNB Edge Datacenters, and TELCO Central Processing', 
              fontsize=16, fontweight='bold')
    
    # Create legend (same style as CloudLogAnalysis.py)
    legend_elements = [
        plt.Rectangle((0, 0), 1, 1, color='green', alpha=0.8, label='PMU Sensors'),
        plt.Circle((0, 0), 1, color='gray', alpha=0.8, label='Edge Datacenters (GNBs)'),
        plt.Polygon([(0, 0), (0.5, 0.866), (1, 0), (0.5, -0.866)], color='lightcoral', alpha=0.9, label='TELCO Hub'),
        plt.Line2D([0], [0], color='purple', linewidth=3, alpha=0.7, label='Network Links'),
        plt.Line2D([0], [0], color='gray', linestyle='--', alpha=0.5, label='PMU-EDGE Links'),
        plt.Circle((0, 0), 1, color='gray', alpha=0.15, label='Coverage Area')
    ]
    
    plt.legend(handles=legend_elements, loc='upper right', fontsize=10)
    
    # Set plot limits
    ax.set_aspect('equal')
    margin = 200
    plt.xlim(-margin, params['length'] + margin)
    plt.ylim(-margin, params['width'] + margin)  # No extra space needed
    
    # Save the plot
    output_path = os.path.join(simulation_folder, SIMULATION_MAP_CHART)
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    logger.info(f"PMU simulation map saved at: {output_path}")
    print(f"PMU simulation map created: {output_path}")

def analyze_pmu_simulation(output_folder: str):
    """Main analysis function for PMU simulation data."""
    simulation_folder = os.path.join(BASE_OUTPUT_DIR, output_folder)
    
    # Setup logging
    logger = setup_logging(simulation_folder)
    
    logger.info("=== PMU Smart Grid Simulation Analysis ===")
    logger.info(f"Analyzing simulation data in folder: {output_folder}")
    
    try:
        # Create the simulation map
        create_simulation_map(simulation_folder, logger)
        
        # **Generate single comprehensive statistics file**
        generate_comprehensive_statistics(simulation_folder, logger)
        
        # **NEW: Export statistics to CSV format**
        export_statistics_to_csv(simulation_folder, logger)
        
        # **NEW: Generate network bandwidth usage charts**
        generate_network_usage_charts(simulation_folder, logger)
        
        logger.info("=== PMU Analysis Complete ===")
        
    except Exception as e:
        logger.error(f"Error during PMU analysis: {str(e)}")
        raise

def generate_network_usage_charts(simulation_folder: str, logger: logging.Logger):
    """Generate network bandwidth usage charts from network usage CSV."""
    logger.info("Generating network bandwidth usage charts...")
    
    try:
        # Look for network usage CSV file
        network_csv = None
        for file in os.listdir(simulation_folder):
            if file.endswith("_network_usage.csv") or file == "Sequential_simulation_network_usage.csv":
                network_csv = os.path.join(simulation_folder, file)
                break
        
        if not network_csv or not os.path.exists(network_csv):
            print("⚠️  Network usage CSV not found - skipping network charts")
            return
        
        print(f"📊 Found network usage CSV: {network_csv}")
        
        # Read network usage data
        import pandas as pd
        df_network = pd.read_csv(network_csv)
        
        # Create network usage charts
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Network Bandwidth Usage Analysis', fontsize=16, fontweight='bold')
        
        # Define order of data flow and colors (only network data transfer, not processing)
        ordered_levels = ['PMU_to_GNB', 'GNB_to_TELCO', 'TELCO_to_GNB']
        level_colors = ['blue', 'lightcoral', 'orange']
        
        # Chart 1: Data Flow Sequence with Control Data (top left)
        if not df_network.empty:
            # Get data in proper order + TSO (only control data)
            ordered_volumes = []
            control_data = []  # Control data for each layer
            labels = []
            
            # Fixed control data size for all layers (constant overhead)
            CONTROL_DATA_SIZE = 2.0  # KB - fixed control overhead per layer
            TSO_CONTROL_SIZE = 0.2   # KB - very small control data for TSO (just for visibility)
            
            for level in ordered_levels:
                if level in df_network['NetworkLevel'].values:
                    volume = df_network[df_network['NetworkLevel'] == level]['TotalDataVolumeKB'].iloc[0]
                    ordered_volumes.append(volume)
                    control_data.append(CONTROL_DATA_SIZE)  # Fixed control data for all layers
                    
                    # Create readable labels
                    if level == 'PMU_to_GNB':
                        labels.append('PMU→GNB')
                    elif level == 'GNB_to_TELCO':
                        labels.append('GNB→TELCO')
                    elif level == 'TELCO_to_GNB':
                        labels.append('TELCO→GNB')
                    else:
                        labels.append(level.replace('_', ' '))
                else:
                    ordered_volumes.append(0)
                    control_data.append(0)
                    labels.append(level.replace('_', ' '))
            
            # Add TSO with only control data (no main data, very small)
            ordered_volumes.append(0)  # No main data for TSO
            control_data.append(TSO_CONTROL_SIZE)   # Very small control data for TSO
            labels.append('TSO Control')
            
            # Create stacked bars with control data (add gray for TSO)
            extended_colors = level_colors + ['gray']  # Add gray for TSO
            extended_dark_colors = ['darkblue', 'darkred', 'darkorange', 'darkgreen', 'darkgray']
            
            bars1_main = ax1.bar(labels, ordered_volumes, color=extended_colors[:len(labels)], label='Main Data')
            bars1_control = ax1.bar(labels, control_data, bottom=ordered_volumes, 
                                  color=extended_dark_colors[:len(labels)], 
                                  label='Control Data', alpha=0.8)
            
            ax1.set_title('Data Flow Sequence (PMU→GNB→TELCO→GNB→Analysis)', fontweight='bold')
            ax1.set_xlabel('Data Flow Step')
            ax1.set_ylabel('Data Volume (KB)')
            ax1.grid(True, alpha=0.3)
            ax1.legend(loc='upper right', fontsize=8)
            
            # Add total value labels on top of bars
            for i, (main_vol, ctrl_vol) in enumerate(zip(ordered_volumes, control_data)):
                total = main_vol + ctrl_vol
                if total > 0:
                    ax1.text(i, total + total*0.02, f'{total:.1f} KB', 
                            ha='center', va='bottom', fontsize=9, fontweight='bold')
        
        # Chart 2: Network Layer Data Distribution (top right)
        # Map data to network infrastructure layers
        layer_data = {
            'Cellular': 0,  # PMU to GNB
            'GNBs': 0,      # GNB processing
            'TELCO': 0,     # TELCO processing  
            'TSO': 0        # TSO (only control data)
        }
        layer_control = {
            'Cellular': CONTROL_DATA_SIZE,  # Fixed control data
            'GNBs': CONTROL_DATA_SIZE * 2,  # GNBs handle 2 connections (GNB_to_TELCO + TELCO_to_GNB)
            'TELCO': CONTROL_DATA_SIZE,     # Fixed control data
            'TSO': TSO_CONTROL_SIZE         # TSO has only very small control data
        }
        
        if not df_network.empty:
            for _, row in df_network.iterrows():
                level = row['NetworkLevel']
                volume = row['TotalDataVolumeKB']
                
                # **FILTER: Only include network transport data, not processing data**
                if level.startswith('StateEstimation_'):
                    continue  # Skip processing data from infrastructure calculations
                
                if level == 'PMU_to_GNB':
                    layer_data['Cellular'] += volume
                elif level in ['GNB_to_TELCO', 'TELCO_to_GNB']:
                    layer_data['GNBs'] += volume
                    # **FIXED: TELCO network also handles the same data that passes through it**
                    layer_data['TELCO'] += volume
                # TSO gets no data - nothing goes there directly
        
        layer_names = list(layer_data.keys())
        layer_volumes = list(layer_data.values())
        layer_ctrl_volumes = list(layer_control.values())
        layer_colors_infra = ['blue', 'lightcoral', 'orange', 'gray']
        layer_ctrl_colors = ['darkblue', 'darkred', 'darkorange', 'darkgray']
        
        bars2_main = ax2.bar(layer_names, layer_volumes, color=layer_colors_infra, label='Data Traffic')
        bars2_control = ax2.bar(layer_names, layer_ctrl_volumes, bottom=layer_volumes, 
                              color=layer_ctrl_colors, label='Control Data', alpha=0.8)
        
        ax2.set_title('Network Infrastructure Data Distribution', fontweight='bold')
        ax2.set_xlabel('Network Layer')
        ax2.set_ylabel('Data Volume (KB)')
        ax2.grid(True, alpha=0.3)
        ax2.legend(loc='upper right', fontsize=8)
        
        # Add total value labels
        for i, (main_vol, ctrl_vol) in enumerate(zip(layer_volumes, layer_ctrl_volumes)):
            total = main_vol + ctrl_vol
            if total > 0:
                ax2.text(i, total + total*0.02, f'{total:.1f} KB', 
                        ha='center', va='bottom', fontsize=9, fontweight='bold')
        
        # Chart 3: GNB Data Volume Distribution (bottom left)
        if not df_network.empty:
            # Extract GNB data volumes from CSV
            gnb_names = []
            gnb_volumes = []
            
            # Find all GNB entries in the CSV
            for _, row in df_network.iterrows():
                level = row['NetworkLevel']
                if level.startswith('GNB_'):
                    gnb_names.append(level)
                    gnb_volumes.append(row['TotalDataVolumeKB'])
            
            # Sort by GNB number for consistent ordering
            if gnb_names and gnb_volumes:
                # Extract GNB numbers for sorting
                gnb_data = list(zip(gnb_names, gnb_volumes))
                gnb_data.sort(key=lambda x: int(x[0].split('_')[1]) if x[0].split('_')[1].isdigit() else 999)
                gnb_names, gnb_volumes = zip(*gnb_data)
                gnb_names = list(gnb_names)
                gnb_volumes = list(gnb_volumes)
                
                # Create color scheme for GNBs (different shades of blue/green)
                gnb_colors = plt.cm.Set3(np.linspace(0, 1, len(gnb_names)))
                
                # Create bar chart for GNB data volumes
                bars3 = ax3.bar(gnb_names, gnb_volumes, color=gnb_colors)
                
                ax3.set_title('GNB Data Volume Distribution', fontweight='bold')
                ax3.set_xlabel('GNB ID')
                ax3.set_ylabel('Total Data Volume (KB)')
                ax3.grid(True, alpha=0.3)
                
                # Add value labels on top of bars
                for bar, volume in zip(bars3, gnb_volumes):
                    height = bar.get_height()
                    ax3.text(bar.get_x() + bar.get_width()/2., height + height*0.02,
                            f'{volume:.0f} KB', ha='center', va='bottom', 
                            fontsize=9, fontweight='bold')
                
                # Rotate x-axis labels if too many GNBs
                if len(gnb_names) > 6:
                    ax3.tick_params(axis='x', rotation=45)
            else:
                # No GNB data found - show empty chart with message
                ax3.text(0.5, 0.5, 'No GNB Data Found', ha='center', va='center', 
                        transform=ax3.transAxes, fontsize=12, color='gray')
                ax3.set_title('GNB Data Volume Distribution', fontweight='bold')
                ax3.set_xlabel('GNB ID')
                ax3.set_ylabel('Total Data Volume (KB)')
        
        # Chart 4: Network Infrastructure Distribution Pie Chart (bottom right)
        if not df_network.empty:
            # Use infrastructure layer data for pie chart
            pie_labels = []
            pie_sizes = []
            pie_colors = []
            
            for layer, volume in layer_data.items():
                total_volume = volume + layer_control[layer]  # Include control data
                if total_volume > 0.1:  # Filter very small values
                    pie_labels.append(layer)
                    pie_sizes.append(total_volume)
                    if layer == 'Cellular':
                        pie_colors.append('blue')
                    elif layer == 'GNBs':
                        pie_colors.append('lightcoral')
                    elif layer == 'TELCO':
                        pie_colors.append('orange')
                    elif layer == 'TSO':
                        pie_colors.append('gray')
            
            if pie_sizes:
                wedges, texts, autotexts = ax4.pie(pie_sizes, labels=pie_labels, colors=pie_colors, 
                                                 autopct='%1.1f%%', startangle=90)
                ax4.set_title('Network Infrastructure Usage Distribution', fontweight='bold')
                
                # Improve text visibility
                for autotext in autotexts:
                    autotext.set_color('white')
                    autotext.set_fontweight('bold')
        
        # Adjust layout to prevent overlap
        plt.tight_layout()
        plt.subplots_adjust(top=0.93)  # Make room for suptitle
        
        # Save the chart
        chart_file = os.path.join(simulation_folder, "network_usage_analysis.png")
        plt.savefig(chart_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"📊 Network usage charts saved to: {chart_file}")
        logger.info(f"Network usage charts saved to: {chart_file}")
        
        return chart_file
        
    except Exception as e:
        print(f"❌ Error creating network usage charts: {e}")
        logger.error(f"Error creating network usage charts: {e}")
        return None

def generate_comprehensive_statistics(simulation_folder: str, logger: logging.Logger):
    """Generate comprehensive statistics file combining PMU data and Grid Analysis analysis."""
    logger.info("Generating Comprehensive Statistics...")
    
    # **Read PMU count from simulation parameters**
    params = read_simulation_parameters()
    max_pmus = params['max_edge_devices']
    
    # Import pandas at the start
    import pandas as pd
    
    comprehensive_report = f"""
=== PMU SMART GRID SIMULATION STATISTICS ===
Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Simulation Folder: {os.path.basename(simulation_folder)}

PMU CONFIGURATION:
- Total PMUs: {max_pmus}
- Simulation Area: {params['length']}m x {params['width']}m
- Edge Coverage: {params['edge_datacenters_coverage']}m

"""
    
    # **SECTION 1: PMU Data Transfer Analysis**
    pmu_csv = None
    for file in os.listdir(simulation_folder):
        if file.endswith("_pmu_data_transfers.csv") or file == "Sequential_simulation_pmu_data_transfers.csv":
            pmu_csv = os.path.join(simulation_folder, file)
            break
    
    # **Get hop averages for additional analysis**
    try:
        _, hop_averages = read_pmu_positions_from_csv(simulation_folder)
    except Exception as e:
        hop_averages = {}
        print(f"Could not get hop averages: {e}")
    
    # **NEW: Get PMU to GNB average times**
    pmu_to_gnb_avg_times = calculate_pmu_to_gnb_average_times(simulation_folder)
    
    if pmu_csv and os.path.exists(pmu_csv):
        try:
            import pandas as pd
            df = pd.read_csv(pmu_csv)
            
            total_transfers = len(df)
            unique_pmus = df['PmuID'].nunique()
            
            # Data size statistics
            avg_data_size = df['DataSize'].mean()
            min_data_size = df['DataSize'].min()
            max_data_size = df['DataSize'].max()
            std_data_size = df['DataSize'].std()
            total_data_volume = df['DataSize'].sum()
            
            # **Add hop averages statistics if available - DISTRIBUTED ARCHITECTURE**
            hop_stats = ""
            if hop_averages:
                # Only PMU->GNB hop exists in distributed architecture
                pmu_to_gnb_times = [avg['gnb'] for avg in hop_averages.values() if avg['gnb'] > 0]
                pmu_to_gnb_distances = [avg['gnb_distance'] for avg in hop_averages.values() if avg['gnb_distance'] > 0]
                
                if pmu_to_gnb_times:
                    avg_pmu_to_gnb = sum(pmu_to_gnb_times) / len(pmu_to_gnb_times)
                    avg_pmu_to_gnb_dist = sum(pmu_to_gnb_distances) / len(pmu_to_gnb_distances) if pmu_to_gnb_distances else 0
                    
                    hop_stats = f"""
AVERAGE HOP DELAY AND DISTANCE (PMU → GNB):
- Average Hop Delay: {avg_pmu_to_gnb:.4f}s
- Average Distance: {avg_pmu_to_gnb_dist:.1f}m
- Total PMU-GNB Connections: {len(pmu_to_gnb_times)}
"""
            
            # **NEW: Generate detailed DATA TASKS DEADLINE MISSED statistics per PMU**
            deadline_missed_details = ""
            if hop_averages:
                # Calculate overall deadline missed statistics
                total_deadline_missed = sum(avg.get('deadline_missed', 0) for avg in hop_averages.values())
                total_all_transfers = sum(avg.get('total_transfers', 0) for avg in hop_averages.values())
                overall_deadline_missed_rate = (total_deadline_missed / total_all_transfers) * 100 if total_all_transfers > 0 else 0
                
                # Count PMUs with deadline misses
                pmus_with_misses = len([avg for avg in hop_averages.values() if avg.get('deadline_missed', 0) > 0])
                
                deadline_missed_details = f"""
PMU DATA MISSED DEADLINE SUMMARY:
- PMU Data that Missed Deadline: {total_deadline_missed}/{total_all_transfers} ({overall_deadline_missed_rate:.2f}%)

DETAILED TRANSFERS PER PMU:"""
                
                # **Read PDC Waiting Times and Total Times from state estimation CSV**
                gnb_pdc_waiting_times = {}
                gnb_total_times = {}
                state_csv = None
                for file in os.listdir(simulation_folder):
                    if file.endswith("_state_estimation.csv") or file == "Sequential_simulation_state_estimation.csv":
                        state_csv = os.path.join(simulation_folder, file)
                        break
                
                if state_csv and os.path.exists(state_csv):
                    import pandas as pd
                    df_state = pd.read_csv(state_csv)
                    
                    # Group by GNBID and calculate average PDCWaitingTime and TotalTime
                    for gnb_id in df_state['GNBID'].unique():
                        gnb_rows = df_state[df_state['GNBID'] == gnb_id]
                        avg_pdc_waiting = gnb_rows['PDCWaitingTime'].mean() if 'PDCWaitingTime' in gnb_rows.columns else 0.0
                        avg_total_time = gnb_rows['TotalTime'].mean() if 'TotalTime' in gnb_rows.columns else 0.0
                        gnb_name = f"GNB_{gnb_id}"
                        gnb_pdc_waiting_times[gnb_name] = avg_pdc_waiting
                        gnb_total_times[gnb_name] = avg_total_time
                
                # **Calculate average transfer delay per PMU from HopSum**
                pmu_avg_transfer_delay = {}
                
                # **Re-analyze CSV to get accurate OK vs total counts per PMU and per GNB**
                pmu_stats_from_csv = {}
                gnb_stats_from_csv = {}
                
                if pmu_csv and os.path.exists(pmu_csv):
                    import pandas as pd
                    df_detailed = pd.read_csv(pmu_csv)
                    
                    # Group by PMU ID to get stats per PMU
                    for pmu_id in df_detailed['PmuID'].unique():
                        pmu_rows = df_detailed[df_detailed['PmuID'] == pmu_id]
                        total_transfers = len(pmu_rows)
                        ok_transfers = len(pmu_rows[pmu_rows['Status'] == 'S'])
                        deadline_missed_transfers = len(pmu_rows[pmu_rows['Status'] == 'L'])
                        

                        
                        # Extract GNB name from path (assuming format "PMU -> GNB_X ...")
                        gnb_name = 'GNB_Unknown'
                        if not pmu_rows.empty and 'Path' in pmu_rows.columns:
                            sample_path = pmu_rows.iloc[0]['Path']
                            if ' -> ' in sample_path:
                                parts = sample_path.split(' -> ')
                                if len(parts) >= 2:
                                    gnb_part = parts[1].strip()
                                    import re
                                    gnb_match = re.match(r'(GNB_\d+)', gnb_part)
                                    if gnb_match:
                                        gnb_name = gnb_match.group(1)
                        
                        pmu_stats_from_csv[pmu_id] = {
                            'ok_count': ok_transfers,
                            'total_count': total_transfers,
                            'gnb_name': gnb_name
                        }
                        
                        # **Calculate average transfer delay for this PMU from HopSum**
                        avg_hop_sum = pmu_rows['HopSum'].mean() if 'HopSum' in pmu_rows.columns else 0.0
                        pmu_avg_transfer_delay[pmu_id] = avg_hop_sum
                        
                        # Accumulate stats per GNB
                        if gnb_name not in gnb_stats_from_csv:
                            gnb_stats_from_csv[gnb_name] = {'ok_count': 0, 'total_count': 0, 'pmu_count': 0}
                        gnb_stats_from_csv[gnb_name]['ok_count'] += ok_transfers
                        gnb_stats_from_csv[gnb_name]['total_count'] += total_transfers
                        gnb_stats_from_csv[gnb_name]['pmu_count'] += 1
                
                # **Display ALL PMUs with correct OK/total counts**
                for pmu_id in sorted(pmu_stats_from_csv.keys()):
                    stats = pmu_stats_from_csv[pmu_id]
                    ok_count = stats['ok_count']
                    total_count = stats['total_count']
                    gnb_name = stats['gnb_name']
                    success_rate = (ok_count / total_count) * 100 if total_count > 0 else 0
                    avg_transfer_delay = pmu_avg_transfer_delay.get(pmu_id, 0.0)
                    
                    # Show successful transfers for all PMUs with average transfer delay
                    deadline_missed_details += f"""
  PMU_{pmu_id:02d} → {gnb_name}: {ok_count}/{total_count} transfers on time ({success_rate:.1f}%) - avg transfer delay: {avg_transfer_delay:.4f}s"""
                
                # **Add summary by GNB using CSV data**
                if gnb_stats_from_csv:
                    deadline_missed_details += f"""

GNB SUMMARY LOGS:"""
                    for gnb, stats in sorted(gnb_stats_from_csv.items()):
                        ok_count = stats['ok_count']
                        total_count = stats['total_count']
                        pmu_count = stats['pmu_count']
                        avg_pdc_waiting_time = gnb_pdc_waiting_times.get(gnb, 0.0)
                        avg_gnb_total_time = gnb_total_times.get(gnb, 0.0)
                        success_rate = (ok_count / total_count) * 100 if total_count > 0 else 0
                        deadline_missed_details += f"""
  {gnb}: {ok_count}/{total_count} transfers on time ({success_rate:.1f}%) from {pmu_count} PMUs - avg PDC waiting time: {avg_pdc_waiting_time:.4f}s - avg TotalTime: {avg_gnb_total_time:.4f}s"""
            
            comprehensive_report += f"""
=== PMU DATA TRANSFER STATISTICS ===

DATA SIZE STATISTICS:
- Average Data Size: {avg_data_size:.2f} KB
- Minimum Data Size: {min_data_size:.2f} KB
- Maximum Data Size: {max_data_size:.2f} KB
- Standard Deviation: {std_data_size:.2f} KB
- Total Data Volume: {total_data_volume:.2f} KB
{hop_stats}
{deadline_missed_details}
"""
        except Exception as e:
            comprehensive_report += f"""
=== PMU DATA TRANSFER STATISTICS ===
ERROR: Could not analyze PMU data transfers: {str(e)}

"""
    else:
        comprehensive_report += f"""
=== PMU DATA TRANSFER STATISTICS ===
WARNING: PMU Data CSV file not found

"""
    
    # **SECTION 2: Grid Analysis**
    state_csv = None
    for file in os.listdir(simulation_folder):
        if file.endswith("_state_estimation.csv") or file == "Sequential_simulation_state_estimation.csv":
            state_csv = os.path.join(simulation_folder, file)
            break
    
    if state_csv and os.path.exists(state_csv):
        try:
            import pandas as pd
            df = pd.read_csv(state_csv)
            
            total_tasks = len(df)
            successful_tasks = len(df[df['SuccessFlag'] == 1])
            failed_tasks = len(df[df['SuccessFlag'] == 0])
            success_rate = (successful_tasks / total_tasks) * 100 if total_tasks > 0 else 0
            
            # Execution time statistics (only for successful tasks)
            successful_df = df[df['SuccessFlag'] == 1]
            if len(successful_df) > 0:
                avg_exec_time = successful_df['ExecTime'].mean()
                min_exec_time = successful_df['ExecTime'].min()
                max_exec_time = successful_df['ExecTime'].max()
                std_exec_time = successful_df['ExecTime'].std()
            else:
                avg_exec_time = min_exec_time = max_exec_time = std_exec_time = 0
            
            # PDC Waiting Time statistics
            avg_pdc_waiting = df['PDCWaitingTime'].mean()
            min_pdc_waiting = df['PDCWaitingTime'].min()
            max_pdc_waiting = df['PDCWaitingTime'].max()
            std_pdc_waiting = df['PDCWaitingTime'].std()
            
            # Total time statistics (from TotalTime column in CSV)
            avg_total_time = df['TotalTime'].mean()
            min_total_time = df['TotalTime'].min()
            max_total_time = df['TotalTime'].max()
            std_total_time = df['TotalTime'].std()
            
            comprehensive_report += f"""
=== GRID ANALYSIS TASK STATISTICS ===

GRID ANALYSIS TASK COMPLETION:
- Total Tasks: {total_tasks}
- Successful: {successful_tasks}
- Failed: {failed_tasks}
- Success Rate: {success_rate:.2f}%

EXECUTION TIME (Successful tasks only):
- Average: {avg_exec_time:.4f}s
- Minimum: {min_exec_time:.4f}s
- Maximum: {max_exec_time:.4f}s
- Standard Deviation: {std_exec_time:.4f}s

PDC WAITING TIME (All tasks):
- Average: {avg_pdc_waiting:.4f}s
- Minimum: {min_pdc_waiting:.4f}s
- Maximum: {max_pdc_waiting:.4f}s
- Standard Deviation: {std_pdc_waiting:.4f}s

TOTAL PROCESSING TIME (All tasks):
- Average: {avg_total_time:.4f}s
- Minimum: {min_total_time:.4f}s
- Maximum: {max_total_time:.4f}s
- Standard Deviation: {std_total_time:.4f}s

BATCH INFORMATION:
- Complete Batches: {len(df[df['BatchType'] == 'COMPLETE'])}
- Timeout Batches: {len(df[df['BatchType'] == 'TIMEOUT'])}
- Complete Batch Rate: {(len(df[df['BatchType'] == 'COMPLETE']) / total_tasks) * 100:.2f}%
- Average PDC Waiting Time: {avg_pdc_waiting:.4f}s

"""
        except Exception as e:
            comprehensive_report += f"""
=== GRID ANALYSIS TASK STATISTICS ===
ERROR: Could not analyze Grid Analysis tasks: {str(e)}

"""
    else:
        comprehensive_report += f"""
=== GRID ANALYSIS TASK STATISTICS ===
WARNING: Grid Analysis CSV file not found

"""
    
    # **SECTION 3: Network Infrastructure Layer Statistics**
    network_csv = None
    for file in os.listdir(simulation_folder):
        if file.endswith("_network_usage.csv") or file == "Sequential_simulation_network_usage.csv":
            network_csv = os.path.join(simulation_folder, file)
            break
    
    if network_csv and os.path.exists(network_csv):
        try:
            import pandas as pd
            df_network = pd.read_csv(network_csv)
            
            # Initialize data volume counters for each network layer
            cellular_data = 0.0  # CELLULAR NETWORK (PMU → GNB)
            gnb_data = 0.0       # GNB NETWORK 
            telco_data = 0.0     # TELCO NETWORK
            tso_data = 0.0       # TSO NETWORK
            
            # Read actual data volumes from CSV
            for _, row in df_network.iterrows():
                level = row['NetworkLevel']
                volume = row['TotalDataVolumeKB']
                
                # **FILTER: Only include network transport data, not processing data**
                if level.startswith('StateEstimation_'):
                    continue  # Skip processing data from infrastructure calculations
                
                # Map network levels to infrastructure layers based on CSV data
                if level == 'PMU_to_GNB':
                    cellular_data += volume
                elif level in ['GNB_to_TELCO', 'TELCO_to_GNB']:
                    gnb_data += volume
                    telco_data += volume  # TELCO handles the same data that passes through it
                elif level == 'TSO':
                    tso_data += volume
                # Look for individual device data (PMU_#, GNB_#, TSO)
                elif level.startswith('PMU_'):
                    cellular_data += volume
                elif level.startswith('GNB_'):
                    gnb_data += volume
                elif level == 'TSO':
                    tso_data += volume
            
            # Calculate control data as 3% of data volume for each layer
            cellular_control = cellular_data * 0.03  # 3% control overhead
            gnb_control = gnb_data * 0.03           # 3% control overhead  
            telco_control = telco_data * 0.03       # 3% control overhead
            tso_control = tso_data * 0.03           # 3% control overhead
            
            # Calculate totals including control data
            cellular_total = cellular_data + cellular_control
            gnb_total = gnb_data + gnb_control
            telco_total = telco_data + telco_control
            tso_total = tso_data + tso_control
            
            comprehensive_report += f"""
=== NETWORK INFRASTRUCTURE LAYER STATISTICS ===

CELLULAR NETWORK (PMU → GNB):
- Data Volume: {cellular_data:.2f} KB
- Control Data Volume: {cellular_control:.2f} KB  
- Total Data Volume: {cellular_total:.2f} KB

GNB NETWORK:
- Data Volume: {gnb_data:.2f} KB
- Control Data Volume: {gnb_control:.2f} KB
- Total Data Volume: {gnb_total:.2f} KB

TELCO NETWORK:
- Data Volume: {telco_data:.2f} KB
- Control Data Volume: {telco_control:.2f} KB
- Total Data Volume: {telco_total:.2f} KB

TSO NETWORK:
- Data Volume: {tso_data:.2f} KB
- Control Data Volume: {tso_control:.2f} KB
- Total Data Volume: {tso_total:.2f} KB

"""
        except Exception as e:
            comprehensive_report += f"""
=== NETWORK INFRASTRUCTURE LAYER STATISTICS ===
ERROR: Could not analyze network layer statistics: {str(e)}

"""
    else:
        comprehensive_report += f"""
=== NETWORK INFRASTRUCTURE LAYER STATISTICS ===
WARNING: Network usage CSV file not found

"""

    comprehensive_report += f"""
========================================
Analysis completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
========================================
"""
    
    # **Save single comprehensive statistics file**
    stats_file = os.path.join(simulation_folder, "simulation_statistics.txt")
    with open(stats_file, 'w') as f:
        f.write(comprehensive_report)
    
    # Log to console and file
    # logger.info(comprehensive_report)  # REMOVED - no file logging
    print(comprehensive_report)
    
    # logger.info(f"Comprehensive statistics saved to: {stats_file}")  # REMOVED
    print(f"📊 Comprehensive statistics saved to: {stats_file}")
    
    return stats_file

def export_statistics_to_csv(simulation_folder: str, logger: logging.Logger):
    """Export comprehensive statistics to simple CSV format for easier analysis."""
    logger.info("Exporting statistics to CSV format...")
    
    # **Read PMU count from simulation parameters**
    params = read_simulation_parameters()
    max_pmus = params['max_edge_devices']
    
    # **Get data for CSV export**
    try:
        _, hop_averages = read_pmu_positions_from_csv(simulation_folder)
        pmu_to_gnb_avg_times = calculate_pmu_to_gnb_average_times(simulation_folder)
    except Exception as e:
        hop_averages = {}
        pmu_to_gnb_avg_times = {}
        print(f"Could not get hop averages for CSV export: {e}")
    
    # **Create simple CSV data structure - Just Metric and Value**
    csv_data = []
    
    # **PMU DATA SECTION**
    csv_data.append(['=== PMU DATA TRANSFER STATISTICS ===', ''])
    
    # **1. Total Data Volume**
    pmu_csv = None
    for file in os.listdir(simulation_folder):
        if file.endswith("_pmu_data_transfers.csv") or file == "Sequential_simulation_pmu_data_transfers.csv":
            pmu_csv = os.path.join(simulation_folder, file)
            break
    
    if pmu_csv and os.path.exists(pmu_csv):
        try:
            import pandas as pd
            df = pd.read_csv(pmu_csv)
            total_data_volume = df['DataSize'].sum()
            csv_data.append(['Total Data Volume', f'{total_data_volume:.2f} KB'])
            
        except Exception as e:
            csv_data.append(['Total Data Volume', 'ERROR'])
    
    csv_data.append(['', ''])  # Empty row for separation
    
    # **2. AVERAGE HOP DELAY AND DISTANCE (PMU → GNB)**
    csv_data.append(['=== AVERAGE HOP DELAY AND DISTANCE (PMU → GNB) ===', ''])
    
    if hop_averages:
        # Only PMU->GNB hop exists in distributed architecture
        pmu_to_gnb_times = [avg['gnb'] for avg in hop_averages.values() if avg['gnb'] > 0]
        pmu_to_gnb_distances = [avg['gnb_distance'] for avg in hop_averages.values() if avg['gnb_distance'] > 0]
        
        if pmu_to_gnb_times:
            avg_pmu_to_gnb = sum(pmu_to_gnb_times) / len(pmu_to_gnb_times)
            avg_pmu_to_gnb_dist = sum(pmu_to_gnb_distances) / len(pmu_to_gnb_distances) if pmu_to_gnb_distances else 0
            
            csv_data.append(['Average Hop Delay', f'{avg_pmu_to_gnb:.4f}s'])
            csv_data.append(['Average Distance', f'{avg_pmu_to_gnb_dist:.1f}m'])
            csv_data.append(['Total PMU-GNB Connections', len(pmu_to_gnb_times)])
    
    csv_data.append(['', ''])  # Empty row
    
    # **3. PMU DATA MISSED DEADLINE SUMMARY**
    csv_data.append(['=== PMU DATA MISSED DEADLINE SUMMARY ===', ''])
    
    if hop_averages:
        total_deadline_missed = sum(avg.get('deadline_missed', 0) for avg in hop_averages.values())
        total_all_transfers = sum(avg.get('total_transfers', 0) for avg in hop_averages.values())
        overall_deadline_missed_rate = (total_deadline_missed / total_all_transfers) * 100 if total_all_transfers > 0 else 0
        pmus_with_misses = len([avg for avg in hop_averages.values() if avg.get('deadline_missed', 0) > 0])
        
        csv_data.append(['PMU Data that Missed Deadline', f'{total_deadline_missed}/{total_all_transfers} ({overall_deadline_missed_rate:.2f}%)'])
    
    csv_data.append(['', ''])  # Empty row
    
    # **4. DETAILED TRANSFERS PER PMU**
    csv_data.append(['=== DETAILED TRANSFERS PER PMU ===', ''])
    
    # **Read PDC Waiting Times and Total Times from state estimation CSV for CSV export**
    gnb_pdc_waiting_times_csv = {}
    gnb_total_times_csv = {}
    state_csv_export = None
    for file in os.listdir(simulation_folder):
        if file.endswith("_state_estimation.csv") or file == "Sequential_simulation_state_estimation.csv":
            state_csv_export = os.path.join(simulation_folder, file)
            break
    
    if state_csv_export and os.path.exists(state_csv_export):
        import pandas as pd
        df_state_export = pd.read_csv(state_csv_export)
        
        # Group by GNBID and calculate average PDCWaitingTime and TotalTime
        for gnb_id in df_state_export['GNBID'].unique():
            gnb_rows = df_state_export[df_state_export['GNBID'] == gnb_id]
            avg_pdc_waiting = gnb_rows['PDCWaitingTime'].mean() if 'PDCWaitingTime' in gnb_rows.columns else 0.0
            avg_total_time = gnb_rows['TotalTime'].mean() if 'TotalTime' in gnb_rows.columns else 0.0
            gnb_name = f"GNB_{gnb_id}"
            gnb_pdc_waiting_times_csv[gnb_name] = avg_pdc_waiting
            gnb_total_times_csv[gnb_name] = avg_total_time
    
    # **Calculate average transfer delay per PMU from HopSum for CSV export**
    pmu_avg_transfer_delay_csv = {}
    
    # **Re-analyze CSV for accurate OK/total counts in CSV export**
    if pmu_csv and os.path.exists(pmu_csv):
        import pandas as pd
        df_csv = pd.read_csv(pmu_csv)
        
        pmu_csv_stats = {}
        gnb_csv_stats = {}
        
        # Group by PMU ID to get stats per PMU
        for pmu_id in sorted(df_csv['PmuID'].unique()):
            pmu_rows = df_csv[df_csv['PmuID'] == pmu_id]
            total_transfers = len(pmu_rows)
            ok_transfers = len(pmu_rows[pmu_rows['Status'] == 'S'])
            deadline_missed_transfers = len(pmu_rows[pmu_rows['Status'] == 'L'])
            

            
            # Extract GNB name from path
            gnb_name = 'GNB_Unknown'
            if not pmu_rows.empty and 'Path' in pmu_rows.columns:
                sample_path = pmu_rows.iloc[0]['Path']
                if ' -> ' in sample_path:
                    parts = sample_path.split(' -> ')
                    if len(parts) >= 2:
                        gnb_part = parts[1].strip()
                        import re
                        gnb_match = re.match(r'(GNB_\d+)', gnb_part)
                        if gnb_match:
                            gnb_name = gnb_match.group(1)
            
            pmu_csv_stats[pmu_id] = {
                'ok_count': ok_transfers,
                'total_count': total_transfers,
                'gnb_name': gnb_name
            }
            
            # **Calculate average transfer delay for this PMU from HopSum for CSV export**
            avg_hop_sum_csv = pmu_rows['HopSum'].mean() if 'HopSum' in pmu_rows.columns else 0.0
            pmu_avg_transfer_delay_csv[pmu_id] = avg_hop_sum_csv
            
            # Accumulate stats per GNB
            if gnb_name not in gnb_csv_stats:
                gnb_csv_stats[gnb_name] = {'ok_count': 0, 'total_count': 0, 'pmu_count': 0}
            gnb_csv_stats[gnb_name]['ok_count'] += ok_transfers
            gnb_csv_stats[gnb_name]['total_count'] += total_transfers
            gnb_csv_stats[gnb_name]['pmu_count'] += 1
        
        # Add PMU details to CSV
        for pmu_id in sorted(pmu_csv_stats.keys()):
            stats = pmu_csv_stats[pmu_id]
            ok_count = stats['ok_count']
            total_count = stats['total_count']
            gnb_name = stats['gnb_name']
            success_rate = (ok_count / total_count) * 100 if total_count > 0 else 0
            avg_transfer_delay_csv = pmu_avg_transfer_delay_csv.get(pmu_id, 0.0)
            
            pmu_detail = f'{ok_count}/{total_count} transfers on time ({success_rate:.1f}%) - avg transfer delay: {avg_transfer_delay_csv:.4f}s'
            csv_data.append([f'PMU_{pmu_id:02d} → {gnb_name}', pmu_detail])
    
    csv_data.append(['', ''])  # Empty row
    
    # **5. GNB SUMMARY LOGS**
    csv_data.append(['=== GNB SUMMARY LOGS ===', ''])
    
    if 'gnb_csv_stats' in locals() and gnb_csv_stats:
        for gnb, stats in sorted(gnb_csv_stats.items()):
            ok_count = stats['ok_count']
            total_count = stats['total_count']
            pmu_count = stats['pmu_count']
            avg_pdc_waiting_time = gnb_pdc_waiting_times_csv.get(gnb, 0.0)
            avg_gnb_total_time = gnb_total_times_csv.get(gnb, 0.0)
            success_rate = (ok_count / total_count) * 100 if total_count > 0 else 0
            gnb_detail = f"{ok_count}/{total_count} transfers on time ({success_rate:.1f}%) from {pmu_count} PMUs - avg PDC waiting time: {avg_pdc_waiting_time:.4f}s - avg TotalTime: {avg_gnb_total_time:.4f}s"
            csv_data.append([gnb, gnb_detail])
    
    csv_data.append(['', ''])  # Empty row
    csv_data.append(['', ''])  # Extra separation
    
    # **GRID ANALYSIS SECTION**
    csv_data.append(['=== GRID ANALYSIS TASK STATISTICS ===', ''])
    
    # **6. GRID ANALYSIS TASK COMPLETION**
    state_csv = None
    for file in os.listdir(simulation_folder):
        if file.endswith("_state_estimation.csv") or file == "Sequential_simulation_state_estimation.csv":
            state_csv = os.path.join(simulation_folder, file)
            break
    
    if state_csv and os.path.exists(state_csv):
        try:
            import pandas as pd
            df = pd.read_csv(state_csv)
            
            total_tasks = len(df)
            successful_tasks = len(df[df['SuccessFlag'] == 1])
            failed_tasks = len(df[df['SuccessFlag'] == 0])
            success_rate = (successful_tasks / total_tasks) * 100 if total_tasks > 0 else 0
            
            csv_data.append(['Total Tasks', total_tasks])
            csv_data.append(['Successful Tasks', successful_tasks])
            csv_data.append(['Failed Tasks', failed_tasks])
            csv_data.append(['Success Rate', f'{success_rate:.2f}%'])
            
            csv_data.append(['', ''])  # Empty row
            
            # **7. EXECUTION TIME (Successful tasks only)**
            csv_data.append(['=== EXECUTION TIME (Successful tasks only) ===', ''])
            successful_df = df[df['SuccessFlag'] == 1]
            if len(successful_df) > 0:
                csv_data.append(['Average Execution Time', f"{successful_df['ExecTime'].mean():.4f}s"])
                csv_data.append(['Minimum Execution Time', f"{successful_df['ExecTime'].min():.4f}s"])
                csv_data.append(['Maximum Execution Time', f"{successful_df['ExecTime'].max():.4f}s"])
                csv_data.append(['Standard Deviation', f"{successful_df['ExecTime'].std():.4f}s"])
            
            csv_data.append(['', ''])  # Empty row
            
            # **8. TOTAL PROCESSING TIME (Average only)**
            csv_data.append(['=== TOTAL PROCESSING TIME ===', ''])
            csv_data.append(['Average Total Processing Time', f"{df['TotalTime'].mean():.4f}s"])
            
            csv_data.append(['', ''])  # Empty row
            
            # **9. BATCH INFORMATION**
            csv_data.append(['=== BATCH INFORMATION ===', ''])
            complete_batches = len(df[df['BatchType'] == 'COMPLETE'])
            timeout_batches = len(df[df['BatchType'] == 'TIMEOUT'])
            complete_batch_rate = (complete_batches / total_tasks) * 100 if total_tasks > 0 else 0
            avg_pdc_waiting = df['PDCWaitingTime'].mean()
            
            csv_data.append(['Complete Batches', complete_batches])
            csv_data.append(['Timeout Batches', timeout_batches])
            csv_data.append(['Complete Batch Rate', f'{complete_batch_rate:.2f}%'])
            csv_data.append(['Average PDC Waiting Time', f'{avg_pdc_waiting:.4f}s'])
            
        except Exception as e:
            csv_data.append(['Grid Analysis Error', str(e)])
    else:
        csv_data.append(['Grid Analysis Data', 'NOT FOUND'])
    
    # **Write simple CSV file**
    csv_file = os.path.join(simulation_folder, "simulation_analysis.csv")
    try:
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            csv_writer = csv.writer(f)
            
            # Write header
            csv_writer.writerow(['Metric', 'Value'])
            
            # Write data
            for row in csv_data:
                csv_writer.writerow(row)
        
        print(f"📊 Statistics exported to CSV: {csv_file}")
        logger.info(f"Statistics exported to CSV: {csv_file}")
        
        # **Create performance charts after CSV export**
        if 'pmu_csv_stats' in locals() and 'pmu_avg_transfer_delay_csv' in locals() and 'gnb_csv_stats' in locals() and 'gnb_pdc_waiting_times_csv' in locals():
            create_performance_charts(
                simulation_folder=simulation_folder,
                pmu_stats=pmu_csv_stats,
                pmu_delays=pmu_avg_transfer_delay_csv,
                gnb_stats=gnb_csv_stats,
                gnb_waiting_times=gnb_pdc_waiting_times_csv,
                logger=logger
            )
        
        return csv_file
        
    except Exception as e:
        print(f"❌ Error writing CSV file: {e}")
        logger.error(f"Error writing CSV file: {e}")
        return None

def create_performance_charts(simulation_folder: str, pmu_stats: dict, pmu_delays: dict, gnb_stats: dict, gnb_waiting_times: dict, logger: logging.Logger):
    """Create 4 performance bar charts: PMU success rates, PMU delays, GNB success rates, GNB waiting times."""
    logger.info("Creating performance charts...")
    
    try:
        # Create 2x2 subplot layout
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('PMU Smart Grid Performance Analysis', fontsize=16, fontweight='bold')
        
        # Chart 1: PMU Success Rates (top left)
        if pmu_stats:
            pmu_ids = sorted(pmu_stats.keys())
            pmu_success_rates = []
            pmu_labels = []
            
            for pmu_id in pmu_ids:
                stats = pmu_stats[pmu_id]
                success_rate = (stats['ok_count'] / stats['total_count']) * 100 if stats['total_count'] > 0 else 0
                pmu_success_rates.append(success_rate)
                pmu_labels.append(f'{pmu_id:02d}')
            
            bars1 = ax1.bar(pmu_labels, pmu_success_rates, color='green')
            ax1.set_title('PMU Transfer Success Rate', fontweight='bold')
            ax1.set_xlabel('PMU ID')
            ax1.set_ylabel('Success Rate (%)')
            ax1.set_ylim(0, 105)
            ax1.grid(True, alpha=0.3)
            
            # Add value labels on bars
            for bar, rate in zip(bars1, pmu_success_rates):
                height = bar.get_height()
                ax1.text(bar.get_x() + bar.get_width()/2., height + 1,
                        f'{rate:.1f}%', ha='center', va='bottom', fontsize=8)
            
            # Rotate x-axis labels if too many PMUs
            if len(pmu_ids) > 10:
                ax1.tick_params(axis='x', rotation=45)
        
        # Chart 2: PMU Average Transfer Delays (top right)  
        if pmu_delays:
            pmu_ids_delay = sorted(pmu_delays.keys())
            pmu_delay_values = []
            pmu_delay_labels = []
            
            for pmu_id in pmu_ids_delay:
                delay = pmu_delays[pmu_id]
                pmu_delay_values.append(delay)
                pmu_delay_labels.append(f'{pmu_id:02d}')
            
            bars2 = ax2.bar(pmu_delay_labels, pmu_delay_values, color='blue')
            ax2.set_title('PMU Average Transfer Delay', fontweight='bold')
            ax2.set_xlabel('PMU ID')
            ax2.set_ylabel('Average Transfer Delay (ms)')
            ax2.grid(True, alpha=0.3)
            
            # Add value labels on bars (convert to ms)
            for bar, delay in zip(bars2, pmu_delay_values):
                height = bar.get_height()
                delay_ms = delay * 1000  # Convert to milliseconds
                ax2.text(bar.get_x() + bar.get_width()/2., height + height*0.02,
                        f'{delay_ms:.0f}ms', ha='center', va='bottom', fontsize=8)
            
            # Rotate x-axis labels if too many PMUs
            if len(pmu_ids_delay) > 10:
                ax2.tick_params(axis='x', rotation=45)
        
        # Chart 3: GNB Success Rates (bottom left)
        if gnb_stats:
            gnb_names = sorted(gnb_stats.keys())
            gnb_success_rates = []
            
            for gnb_name in gnb_names:
                stats = gnb_stats[gnb_name]
                success_rate = (stats['ok_count'] / stats['total_count']) * 100 if stats['total_count'] > 0 else 0
                gnb_success_rates.append(success_rate)
            
            bars3 = ax3.bar(gnb_names, gnb_success_rates, color='red')
            ax3.set_title('GNB Transfer Success Rate', fontweight='bold')
            ax3.set_xlabel('GNB ID')
            ax3.set_ylabel('Success Rate (%)')
            ax3.set_ylim(0, 105)
            ax3.grid(True, alpha=0.3)
            
            # Add value labels on bars
            for bar, rate in zip(bars3, gnb_success_rates):
                height = bar.get_height()
                ax3.text(bar.get_x() + bar.get_width()/2., height + 1,
                        f'{rate:.1f}%', ha='center', va='bottom', fontsize=10)
        
        # Chart 4: GNB Average Timings (bottom right) - Stacked bar chart
        if gnb_waiting_times:
            # Read state estimation CSV to get execution times per GNB
            state_csv = None
            for file in os.listdir(simulation_folder):
                if file.endswith("_state_estimation.csv") or file == "Sequential_simulation_state_estimation.csv":
                    state_csv = os.path.join(simulation_folder, file)
                    break
            
            gnb_exec_times = {}
            gnb_total_times_chart = {}
            
            if state_csv and os.path.exists(state_csv):
                import pandas as pd
                df_state = pd.read_csv(state_csv)
                
                # Calculate average execution times and total times per GNB
                for gnb_id in df_state['GNBID'].unique():
                    gnb_rows = df_state[df_state['GNBID'] == gnb_id]
                    avg_exec_time = gnb_rows['ExecTime'].mean() if 'ExecTime' in gnb_rows.columns else 0.0
                    avg_total_time = gnb_rows['TotalTime'].mean() if 'TotalTime' in gnb_rows.columns else 0.0
                    gnb_name = f"GNB_{gnb_id}"
                    gnb_exec_times[gnb_name] = avg_exec_time
                    gnb_total_times_chart[gnb_name] = avg_total_time
            
            gnb_names_wait = sorted(gnb_waiting_times.keys())
            gnb_pdc_times = []
            gnb_network_times = []
            gnb_return_network_times = []  # NEW: Return Network Time
            gnb_exec_times_list = []
            
            for gnb_name in gnb_names_wait:
                pdc_time = gnb_waiting_times[gnb_name]
                exec_time = gnb_exec_times.get(gnb_name, 0.0)
                total_time = gnb_total_times_chart.get(gnb_name, 0.0)
                network_time = max(0, total_time - pdc_time - exec_time)  # Ensure non-negative
                
                gnb_pdc_times.append(pdc_time)
                gnb_network_times.append(network_time)
                gnb_return_network_times.append(network_time)  # Return time = Network time
                gnb_exec_times_list.append(exec_time)
            
            # Create stacked bar chart (Network Time → PDC Waiting Time → Execution Time → Return Network Time)
            bars4_network = ax4.bar(gnb_names_wait, gnb_network_times, color='blue', label='Network Time', width=0.6)
            bars4_pdc = ax4.bar(gnb_names_wait, gnb_pdc_times, bottom=gnb_network_times, color='lightcoral', label='PDC Waiting Time', width=0.6)
            bars4_exec = ax4.bar(gnb_names_wait, gnb_exec_times_list, 
                               bottom=[n + p for n, p in zip(gnb_network_times, gnb_pdc_times)], 
                               color='orange', label='Execution Time', width=0.6)
            bars4_return = ax4.bar(gnb_names_wait, gnb_return_network_times, 
                                  bottom=[n + p + e for n, p, e in zip(gnb_network_times, gnb_pdc_times, gnb_exec_times_list)], 
                                  color='lightblue', label='Return Network Time', width=0.6)
            
            ax4.set_title('GNB Average Timings', fontweight='bold')
            ax4.set_xlabel('GNB ID')
            ax4.set_ylabel('Average Time (ms)')
            ax4.grid(True, alpha=0.3)
            ax4.legend(loc='upper right', fontsize=8)
            
            # Set Y-axis limit to 0.25 for better visual comparison
            ax4.set_ylim(0, 0.25)
            
            # Add total time labels on top of bars (convert to ms)
            for i, gnb_name in enumerate(gnb_names_wait):
                total_time = gnb_network_times[i] + gnb_pdc_times[i] + gnb_exec_times_list[i] + gnb_return_network_times[i]
                if total_time > 0:
                    total_time_ms = total_time * 1000  # Convert to milliseconds
                    ax4.text(i, total_time + total_time*0.02,
                            f'{total_time_ms:.0f}ms', ha='center', va='bottom', fontsize=9, fontweight='bold')
        
        # Adjust layout to prevent overlap
        plt.tight_layout()
        plt.subplots_adjust(top=0.93)  # Make room for suptitle
        
        # Save the chart
        chart_file = os.path.join(simulation_folder, "performance_analysis_charts.png")
        plt.savefig(chart_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"📊 Performance charts saved to: {chart_file}")
        logger.info(f"Performance charts saved to: {chart_file}")
        
        return chart_file
        
    except Exception as e:
        print(f"❌ Error creating performance charts: {e}")
        logger.error(f"Error creating performance charts: {e}")
        return None

def main():
    """Main entry point for the PMU analysis script."""
    parser = argparse.ArgumentParser(description='Analyze PMU Smart Grid simulation data')
    parser.add_argument('output_folder', help='The output folder containing the simulation data')
    args = parser.parse_args()
    
    print(f"=== PMU Log Analysis Starting ===")
    print(f"Output folder: {args.output_folder}")
    
    # Run the analysis
    analyze_pmu_simulation(args.output_folder)
    
    print(f"=== PMU Log Analysis Complete ===")

if __name__ == "__main__":
    main() 