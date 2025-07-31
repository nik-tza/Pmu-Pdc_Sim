#!/usr/bin/env python3
"""
PMU Log Analysis Script
Analyzes PMU Smart Grid simulation data and creates visualization maps.
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
BASE_OUTPUT_DIR = "UpfOnTelco_PdcOnCloud/output"
PMU_SETTINGS_DIR = "UpfOnTelco_PdcOnCloud/settings"
SHOW_PLOTS = True

# Output files
SIMULATION_MAP_CHART = "cloud_simulation_map.png"

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
        # **FIXED: Only look for Sequential_simulation_pmu.csv**
        pmu_csv_file = os.path.join(simulation_folder, "Sequential_simulation_pmu.csv")
        
        if not os.path.exists(pmu_csv_file):
            print(f"ERROR: Sequential_simulation_pmu.csv not found at: {pmu_csv_file}")
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
        
        # **Read the CSV file using pandas DataFrame for better column handling**
        import pandas as pd
        df = pd.read_csv(pmu_csv_file)
        print(f"✓ CSV header: {list(df.columns)}")
        
        # **Verify we have the expected columns**
        expected_columns = ['Time', 'PmuID', 'PmuCoordinates', 'DataSize', 'GNB_Target', 'Path', 'HopSum', 'Status']
        missing_columns = [col for col in expected_columns if col not in df.columns]
        if missing_columns:
            print(f"❌ ERROR: Missing columns: {missing_columns}")
            return [], {}
        
        print(f"✅ Found all expected columns: {list(df.columns)}")
        
        # **Process data rows using DataFrame**
        processed_entries = 0
        processed_pmus = 0
        
        for index, row in df.iterrows():
            try:
                # **Extract all fields using column names**
                time_val = float(row['Time'])
                pmu_id = int(row['PmuID'])
                pmu_coordinates = str(row['PmuCoordinates']).strip('"')  # Remove quotes if present
                data_size = float(row['DataSize'])
                gnb_target = str(row['GNB_Target']).strip()  # GNB_Target column
                path = str(row['Path']).strip('"')  # Remove quotes if present  
                hop_sum = float(row['HopSum'])
                status = str(row['Status']).strip()  # Status column (OK or DEADLINE_MISSED)
                
                # **Check for DEADLINE_MISSED in the Status column**
                deadline_missed = (status == 'DEADLINE_MISSED')
                
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
                            gnb_part = path_parts[1].strip()  # "GNB_2 (0.0182s, 409.6m)"
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
                            # Only set GNB name if not already set (keep the first one)
                            if 'gnb_name' not in hop_times[pmu_id] or not hop_times[pmu_id]['gnb_name']:
                                hop_times[pmu_id]['gnb_name'] = gnb_name
                                # Debug print for first few entries
                                if processed_entries <= 10:
                                    print(f"🎯 PMU {pmu_id}: FIRST assignment to {gnb_name} (from path: {path})")
                            elif processed_entries <= 10:
                                print(f"🔄 PMU {pmu_id}: keeping original assignment to {hop_times[pmu_id]['gnb_name']} (ignoring {gnb_name})")
                
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
                    print(f"❌ Error processing row {index}: {e}")
                continue
                
                # Stop after processing enough rows to get all unique PMUs
                if processed_entries >= 1000:
                    break
            
            print(f"✅ Processed {processed_entries} entries: {processed_pmus} unique PMUs found")
            
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

def calculate_accurate_transfer_stats(simulation_folder: str) -> Dict[str, Dict]:
    """Calculate accurate transfer statistics per PMU and per GNB from Sequential_simulation_pmu.csv."""
    pmu_stats = {}  # {pmu_id: {'ok': count, 'total': count, 'gnb_name': str, 'hop_sum_times': []}}
    gnb_stats = {}  # {gnb_name: {'ok': count, 'total': count}}
    
    try:
        pmu_csv_file = os.path.join(simulation_folder, "Sequential_simulation_pmu.csv")
        
        if not os.path.exists(pmu_csv_file):
            print(f"WARNING: Sequential_simulation_pmu.csv not found for accurate transfer stats")
            return {'pmu_stats': pmu_stats, 'gnb_stats': gnb_stats}
        
        print("📊 Calculating accurate transfer statistics per PMU and GNB from CSV...")
        
        with open(pmu_csv_file, 'r') as file:
            csv_reader = csv.reader(file)
            
            # Skip header
            next(csv_reader)
            
            # Process all rows
            for line_num, row in enumerate(csv_reader, start=2):
                try:
                    if len(row) != 8:
                        continue
                    
                    # Extract data from CSV row
                    pmu_id = int(row[1])  # PmuID column
                    path = row[5].strip('"')  # Path column (moved to position 5 due to GNB_Target)
                    hop_sum = float(row[6])  # HopSum column (moved to position 6)
                    status = row[7].strip()  # Status column (moved to position 7)
                    
                    # Extract GNB name from path
                    # Format: "PMU -> GNB_2 (0.0001s, 110.1m) -> TELCO (...) -> TSO (...)"
                    gnb_name = None
                    if '->' in path:
                        path_parts = path.split(' -> ')
                        if len(path_parts) >= 2:
                            gnb_part = path_parts[1].strip()  # "GNB_2 (0.0001s, 110.1m)"
                            # Extract GNB name (everything before the first space or parenthesis)
                            import re
                            gnb_match = re.match(r'(GNB_\d+|GNB_Unknown)', gnb_part)
                            if gnb_match:
                                gnb_name = gnb_match.group(1)
                    
                    # If we couldn't extract GNB name, skip this row
                    if not gnb_name:
                        continue
                    
                    # Initialize PMU stats if not exists
                    if pmu_id not in pmu_stats:
                        pmu_stats[pmu_id] = {'ok': 0, 'total': 0, 'gnb_name': gnb_name, 'hop_sum_times': []}
                    
                    # Initialize GNB stats if not exists
                    if gnb_name not in gnb_stats:
                        gnb_stats[gnb_name] = {'ok': 0, 'total': 0}
                    
                    # Update total counts
                    pmu_stats[pmu_id]['total'] += 1
                    gnb_stats[gnb_name]['total'] += 1
                    
                    # Store hop sum time for this PMU
                    pmu_stats[pmu_id]['hop_sum_times'].append(hop_sum)
                    
                    # Update OK counts if status is OK
                    if status == 'OK':
                        pmu_stats[pmu_id]['ok'] += 1
                        gnb_stats[gnb_name]['ok'] += 1
                    
                    # Update GNB name for this PMU (in case it changes, though it shouldn't)
                    pmu_stats[pmu_id]['gnb_name'] = gnb_name
                
                except (ValueError, IndexError) as e:
                    continue
        
        # Debug output
        print(f"✅ Processed transfer stats: {len(pmu_stats)} PMUs, {len(gnb_stats)} GNBs")
        
        # Calculate average hop sum times for each PMU
        for pmu_id, stats in pmu_stats.items():
            if stats['hop_sum_times']:
                stats['avg_hop_sum'] = sum(stats['hop_sum_times']) / len(stats['hop_sum_times'])
            else:
                stats['avg_hop_sum'] = 0.0
        
        # Show first few PMUs for debugging
        for pmu_id in sorted(list(pmu_stats.keys())[:5]):
            stats = pmu_stats[pmu_id]
            print(f"  PMU {pmu_id} → {stats['gnb_name']}: {stats['ok']}/{stats['total']} OK transfers, avg total time: {stats['avg_hop_sum']:.4f}s")
        
        # Show GNB stats for debugging
        for gnb_name in sorted(gnb_stats.keys()):
            stats = gnb_stats[gnb_name]
            print(f"  {gnb_name}: {stats['ok']}/{stats['total']} OK transfers")
        
        return {'pmu_stats': pmu_stats, 'gnb_stats': gnb_stats}
        
    except Exception as e:
        print(f"❌ Error calculating accurate transfer stats: {e}")
        return {'pmu_stats': pmu_stats, 'gnb_stats': gnb_stats}

def calculate_overall_deadline_missed(simulation_folder: str) -> Dict[str, any]:
    """Calculate overall deadline missed statistics from the entire Sequential_simulation_pmu.csv file."""
    stats = {
        'total_transfers': 0,
        'deadline_missed': 0,
        'deadline_missed_rate': 0.0
    }
    
    try:
        pmu_csv_file = os.path.join(simulation_folder, "Sequential_simulation_pmu.csv")
        
        if not os.path.exists(pmu_csv_file):
            print(f"WARNING: Sequential_simulation_pmu.csv not found for overall deadline missed calculation")
            return stats
        
        print("📊 Calculating overall deadline missed statistics from entire CSV...")
        
        with open(pmu_csv_file, 'r') as file:
            csv_reader = csv.reader(file)
            
            # Skip header
            next(csv_reader)
            
            # Count all transfers
            for line_num, row in enumerate(csv_reader, start=2):
                try:
                    if len(row) != 7:
                        continue
                    
                    # Count this transfer
                    stats['total_transfers'] += 1
                    
                    # Check if deadline missed
                    status = row[6].strip()  # Status column (OK or DEADLINE_MISSED)
                    if status == 'DEADLINE_MISSED':
                        stats['deadline_missed'] += 1
                
                except (ValueError, IndexError) as e:
                    continue
        
        # Calculate rate
        if stats['total_transfers'] > 0:
            stats['deadline_missed_rate'] = (stats['deadline_missed'] / stats['total_transfers']) * 100
        
        print(f"✅ Overall deadline missed: {stats['deadline_missed']}/{stats['total_transfers']} ({stats['deadline_missed_rate']:.2f}%)")
        return stats
        
    except Exception as e:
        print(f"❌ Error calculating overall deadline missed: {e}")
        return stats

def calculate_pmu_to_gnb_average_times(simulation_folder: str) -> Dict[int, float]:
    """Calculate average PMU to GNB transfer times from Sequential_simulation_pmu.csv."""
    pmu_to_gnb_times = {}  # {pmu_id: [list of times]}
    
    try:
        pmu_csv_file = os.path.join(simulation_folder, "Sequential_simulation_pmu.csv")
        
        if not os.path.exists(pmu_csv_file):
            print(f"WARNING: Sequential_simulation_pmu.csv not found for PMU-GNB time calculation")
            return {}
        
        print("📊 Calculating PMU to GNB average transfer times...")
        
        with open(pmu_csv_file, 'r') as file:
            csv_reader = csv.reader(file)
            
            # Skip header
            next(csv_reader)
            
            for line_num, row in enumerate(csv_reader, start=2):
                try:
                    if len(row) != 8:
                        continue
                    
                    pmu_id = int(row[1])
                    path = row[5].strip('"')  # Path column (moved to position 5 due to GNB_Target)
                    
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
                
                # Process enough entries to get good statistics
                if line_num > 2000:  # Process more entries for better statistics
                    break
        
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

def calculate_gnb_network_times(simulation_folder: str) -> Dict[str, float]:
    """Calculate average network times per GNB from HopSum values in Sequential_simulation_pmu.csv."""
    gnb_network_times = {}
    
    try:
        pmu_csv_file = os.path.join(simulation_folder, "Sequential_simulation_pmu.csv")
        
        if not os.path.exists(pmu_csv_file):
            print(f"WARNING: Sequential_simulation_pmu.csv not found for GNB network times calculation")
            return gnb_network_times
        
        print("📊 Calculating GNB network times from HopSum values...")
        
        # Read CSV using pandas DataFrame for better column handling
        import pandas as pd
        df = pd.read_csv(pmu_csv_file)
        
        for index, row in df.iterrows():
            try:
                gnb_target = str(row['GNB_Target']).strip()
                hop_sum = float(row['HopSum'])
                
                if gnb_target not in gnb_network_times:
                    gnb_network_times[gnb_target] = []
                
                gnb_network_times[gnb_target].append(hop_sum)
            
            except (ValueError, IndexError) as e:
                continue
        
        # Calculate averages for each GNB
        gnb_averages = {}
        for gnb_name, times in gnb_network_times.items():
            if times:
                avg_time = sum(times) / len(times)
                gnb_averages[gnb_name] = avg_time
                print(f"  {gnb_name}: {len(times)} samples, avg network time: {avg_time:.4f}s")
        
        print(f"✅ Calculated average network times for {len(gnb_averages)} GNBs")
        return gnb_averages
        
    except Exception as e:
        print(f"❌ Error calculating GNB network times: {e}")
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
        
        # Choose color and shape based on type
        if name == "TELCO":
            color = 'lightcoral'
            # Create hexagon for TELCO
            hexagon = plt.Polygon([
                (x - TELCO_SIZE, y),
                (x - TELCO_SIZE/2, y + TELCO_SIZE*0.866),
                (x + TELCO_SIZE/2, y + TELCO_SIZE*0.866),
                (x + TELCO_SIZE, y),
                (x + TELCO_SIZE/2, y - TELCO_SIZE*0.866),
                (x - TELCO_SIZE/2, y - TELCO_SIZE*0.866)
            ], color=color, alpha=0.9, ec='black', linewidth=2)
            ax.add_patch(hexagon)
        else:
            color = colors[i]
            # Regular circle for EDGE datacenters
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
    
    # Add TSO cloud datacenter (positioned above the map)
    tso_x = params['length'] / 2
    tso_y = params['width'] + 100
    datacenter_positions['TSO'] = (tso_x, tso_y)
    
    # Create octagon for TSO
    tso_octagon = plt.Polygon([
        (tso_x - TSO_SIZE, tso_y),
        (tso_x - TSO_SIZE*0.707, tso_y + TSO_SIZE*0.707),
        (tso_x, tso_y + TSO_SIZE),
        (tso_x + TSO_SIZE*0.707, tso_y + TSO_SIZE*0.707),
        (tso_x + TSO_SIZE, tso_y),
        (tso_x + TSO_SIZE*0.707, tso_y - TSO_SIZE*0.707),
        (tso_x, tso_y - TSO_SIZE),
        (tso_x - TSO_SIZE*0.707, tso_y - TSO_SIZE*0.707)
    ], color='blue', alpha=0.9, ec='black', linewidth=2)
    ax.add_patch(tso_octagon)
    
    plt.annotate('TSO (Cloud)', (tso_x, tso_y), xytext=(5, 5), textcoords='offset points',
               fontweight='bold', fontsize=14,
               bbox=dict(boxstyle="round,pad=0.3", facecolor='lightblue', alpha=0.9))
    
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
    
    # **NEW: Plot network connections between datacenters with average timing**
    # Calculate average GNB->TELCO and TELCO->TSO times across all PMUs
    gnb_to_telco_times = []
    telco_to_tso_times = []
    
    for pmu_id, averages in hop_averages.items():
        if averages['gnb_to_telco'] > 0:
            gnb_to_telco_times.append(averages['gnb_to_telco'])
        if averages['telco_to_tso'] > 0:
            telco_to_tso_times.append(averages['telco_to_tso'])
    
    avg_gnb_to_telco = sum(gnb_to_telco_times) / len(gnb_to_telco_times) if gnb_to_telco_times else 0.0
    avg_telco_to_tso = sum(telco_to_tso_times) / len(telco_to_tso_times) if telco_to_tso_times else 0.0
    
    for link in links:
        from_node = link['from']
        to_node = link['to']
        
        # Get positions
        from_pos = datacenter_positions.get(from_node)
        to_pos = datacenter_positions.get(to_node)
        
        if from_pos and to_pos:
            x1, y1 = from_pos
            x2, y2 = to_pos
            
            # Draw connection line
            plt.plot([x1, x2], [y1, y2], 'purple', linestyle='-', 
                    linewidth=3, alpha=0.7)
            
            # **Add average timing labels on network links**
            mid_x = (x1 + x2) / 2
            mid_y = (y1 + y2) / 2
            
            # Determine which average to show based on connection type
            if to_node == "TELCO" and from_node.startswith("EDGE_"):
                # GNB->TELCO connection
                plt.annotate(f'{avg_gnb_to_telco:.3f}s', (mid_x, mid_y), 
                           xytext=(3, 3), textcoords='offset points',
                           fontsize=7, fontweight='bold', color='purple',
                           ha='center', va='center',
                           bbox=dict(boxstyle="round,pad=0.1", facecolor='plum', alpha=0.7))
            elif from_node == "TELCO" and to_node == "TSO":
                # TELCO->TSO connection
                plt.annotate(f'{avg_telco_to_tso:.3f}s', (mid_x, mid_y), 
                           xytext=(3, 3), textcoords='offset points',
                           fontsize=7, fontweight='bold', color='purple',
                           ha='center', va='center',
                           bbox=dict(boxstyle="round,pad=0.1", facecolor='plum', alpha=0.7))
    
    # Plot settings
    plt.grid(True, linestyle='--', alpha=0.3)
    plt.xlabel('X Coordinate (meters)', fontsize=12)
    plt.ylabel('Y Coordinate (meters)', fontsize=12)
    plt.title(f'PMU Smart Grid Simulation Map\n{max_pmus} PMU Sensors, GNBs, TELCO and TSO Cloud', 
              fontsize=16, fontweight='bold')
    
    # Create legend
    legend_elements = [
        plt.Rectangle((0, 0), 1, 1, color='green', alpha=0.8, label='PMU Sensors'),
        plt.Circle((0, 0), 1, color='gray', alpha=0.8, label='Edge Datacenters (GNBs)'),
        plt.Polygon([(0, 0), (0.5, 0.866), (1, 0), (0.5, -0.866)], color='lightcoral', alpha=0.9, label='TELCO Hub'),
        plt.Polygon([(0, 0), (0.707, 0.707), (1, 0), (0.707, -0.707), (0, -1), (-0.707, -0.707), (-1, 0), (-0.707, 0.707)], 
                   color='blue', alpha=0.9, label='TSO Cloud'),
        plt.Line2D([0], [0], color='purple', linewidth=3, alpha=0.7, label='Network Links'),
        plt.Line2D([0], [0], color='gray', linestyle='--', alpha=0.5, label='PMU-EDGE Links'),
        plt.Circle((0, 0), 1, color='gray', alpha=0.15, label='Coverage Area')
    ]
    
    plt.legend(handles=legend_elements, loc='upper right', fontsize=10)
    
    # Set plot limits
    ax.set_aspect('equal')
    margin = 200
    plt.xlim(-margin, params['length'] + margin)
    plt.ylim(-margin, params['width'] + margin + 200)  # Extra space for TSO
    
    # Save the plot
    output_path = os.path.join(simulation_folder, SIMULATION_MAP_CHART)
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    logger.info(f"PMU simulation map saved at: {output_path}")
    print(f"PMU simulation map created: {output_path}")

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
        fig.suptitle('Cloud Network Bandwidth Usage Analysis', fontsize=16, fontweight='bold')
        
        # Define order of data flow and colors (for cloud scenario: PMU->GNB->TELCO->TSO)
        ordered_levels = ['PMU_to_GNB', 'GNB_to_TELCO', 'TELCO_to_TSO']
        level_colors = ['blue', 'lightcoral', 'orange']
        
        # Chart 1: Data Flow Sequence with Control Data (top left)
        if not df_network.empty:
            # Get data in proper order
            ordered_volumes = []
            control_data = []  # Control data for each layer
            labels = []
            
            # Fixed control data size for all layers (constant overhead)
            CONTROL_DATA_SIZE = 2.0  # KB - fixed control overhead per layer
            TSO_CONTROL_SIZE = 0.5   # KB - control data for TSO processing
            
            for level in ordered_levels:
                if level in df_network['NetworkLevel'].values:
                    volume = df_network[df_network['NetworkLevel'] == level]['TotalDataVolumeKB'].iloc[0]
                    ordered_volumes.append(volume)
                    
                    if level == 'TELCO_to_TSO':
                        control_data.append(TSO_CONTROL_SIZE)  # TSO has processing control data
                    else:
                        control_data.append(CONTROL_DATA_SIZE)  # Fixed control data for network layers
                    
                    # Create readable labels
                    if level == 'PMU_to_GNB':
                        labels.append('PMU→GNB')
                    elif level == 'GNB_to_TELCO':
                        labels.append('GNB→TELCO')
                    elif level == 'TELCO_to_TSO':
                        labels.append('TELCO→TSO')
                    else:
                        labels.append(level.replace('_', ' '))
                else:
                    ordered_volumes.append(0)
                    control_data.append(0)
                    if level == 'PMU_to_GNB':
                        labels.append('PMU→GNB')
                    elif level == 'GNB_to_TELCO':
                        labels.append('GNB→TELCO')
                    elif level == 'TELCO_to_TSO':
                        labels.append('TELCO→TSO')
                    else:
                        labels.append(level.replace('_', ' '))
            
            # Create stacked bars with control data
            extended_colors = level_colors[:len(labels)]
            extended_dark_colors = ['darkblue', 'darkred', 'darkorange']
            
            bars1_main = ax1.bar(labels, ordered_volumes, color=extended_colors, label='Main Data')
            bars1_control = ax1.bar(labels, control_data, bottom=ordered_volumes, 
                                  color=extended_dark_colors[:len(labels)], 
                                  label='Control Data', alpha=0.8)
            
            ax1.set_title('Cloud Data Flow Sequence (PMU→GNB→TELCO→TSO)', fontweight='bold')
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
        
        # Chart 2: Network Infrastructure Data Distribution (top right)
        # Map data to network infrastructure layers for cloud scenario
        layer_data = {
            'Cellular': 0,  # PMU to GNB
            'GNBs': 0,      # GNB processing
            'TELCO': 0,     # TELCO processing  
            'TSO Cloud': 0  # TSO cloud processing
        }
        layer_control = {
            'Cellular': CONTROL_DATA_SIZE,  # Fixed control data
            'GNBs': CONTROL_DATA_SIZE,      # GNB control data
            'TELCO': CONTROL_DATA_SIZE,     # TELCO control data
            'TSO Cloud': TSO_CONTROL_SIZE   # TSO processing control data
        }
        
        if not df_network.empty:
            for _, row in df_network.iterrows():
                level = row['NetworkLevel']
                volume = row['TotalDataVolumeKB']
                
                if level == 'PMU_to_GNB':
                    layer_data['Cellular'] += volume
                elif level == 'GNB_to_TELCO':
                    layer_data['GNBs'] += volume
                    layer_data['TELCO'] += volume  # TELCO also handles this data
                elif level == 'TELCO_to_TSO':
                    layer_data['TSO Cloud'] += volume
        
        layer_names = list(layer_data.keys())
        layer_volumes = list(layer_data.values())
        layer_ctrl_volumes = list(layer_control.values())
        layer_colors_infra = ['blue', 'lightcoral', 'orange', 'purple']
        layer_ctrl_colors = ['darkblue', 'darkred', 'darkorange', 'darkmagenta']
        
        bars2_main = ax2.bar(layer_names, layer_volumes, color=layer_colors_infra, label='Data Traffic')
        bars2_control = ax2.bar(layer_names, layer_ctrl_volumes, bottom=layer_volumes, 
                              color=layer_ctrl_colors, label='Control Data', alpha=0.8)
        
        ax2.set_title('Cloud Network Infrastructure Data Distribution', fontweight='bold')
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
        
        # Chart 4: Cloud Processing Distribution Pie Chart (bottom right)
        if not df_network.empty:
            # Use infrastructure layer data for pie chart
            pie_labels = []
            pie_sizes = []
            pie_colors = []
            
            for layer, volume in layer_data.items():
                if volume > 0:
                    pie_labels.append(layer)
                    pie_sizes.append(volume)
            
            if pie_sizes:
                # Use colors matching the infrastructure chart
                pie_colors = layer_colors_infra[:len(pie_labels)]
                
                # Create pie chart
                wedges, texts, autotexts = ax4.pie(pie_sizes, labels=pie_labels, colors=pie_colors,
                                                  autopct='%1.1f%%', startangle=90)
                
                ax4.set_title('Cloud Infrastructure Data Distribution', fontweight='bold')
                
                # Enhance text readability
                for autotext in autotexts:
                    autotext.set_color('white')
                    autotext.set_fontweight('bold')
            else:
                # No data for pie chart
                ax4.text(0.5, 0.5, 'No Data Available', ha='center', va='center', 
                        transform=ax4.transAxes, fontsize=12, color='gray')
                ax4.set_title('Cloud Infrastructure Data Distribution', fontweight='bold')
        
        # Adjust layout and save
        plt.tight_layout()
        
        # Save the charts
        output_path = os.path.join(simulation_folder, "network_bandwidth_usage_charts.png")
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Network bandwidth usage charts saved at: {output_path}")
        print(f"📊 Network bandwidth usage charts created: {output_path}")
        
    except Exception as e:
        logger.error(f"Error creating network usage charts: {str(e)}")
        print(f"❌ Error creating network usage charts: {e}")

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
        
        # **Generate network bandwidth usage charts**
        generate_network_usage_charts(simulation_folder, logger)
        
        # **NEW: Create performance analysis charts**
        create_performance_charts(simulation_folder, logger)
        
        # **Generate single comprehensive statistics file**
        generate_comprehensive_statistics(simulation_folder, logger)
        
        # **NEW: Export statistics to CSV format**
        export_statistics_to_csv(simulation_folder, logger)
        
        logger.info("=== PMU Analysis Complete ===")
        
    except Exception as e:
        logger.error(f"Error during PMU analysis: {str(e)}")
        raise

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
        if file.endswith("_pmu.csv"):
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
            
            # **Add hop averages statistics if available**
            hop_stats = ""
            if hop_averages:
                avg_pmu_to_gnb = sum(avg['gnb'] for avg in hop_averages.values() if avg['gnb'] > 0) / len([avg for avg in hop_averages.values() if avg['gnb'] > 0])
                avg_gnb_to_telco = sum(avg['gnb_to_telco'] for avg in hop_averages.values() if avg['gnb_to_telco'] > 0) / len([avg for avg in hop_averages.values() if avg['gnb_to_telco'] > 0])
                avg_telco_to_tso = sum(avg['telco_to_tso'] for avg in hop_averages.values() if avg['telco_to_tso'] > 0) / len([avg for avg in hop_averages.values() if avg['telco_to_tso'] > 0])
                
                # **NEW: Calculate average distances**
                avg_pmu_to_gnb_dist = sum(avg['gnb_distance'] for avg in hop_averages.values() if avg['gnb_distance'] > 0) / len([avg for avg in hop_averages.values() if avg['gnb_distance'] > 0])
                avg_gnb_to_telco_dist = sum(avg['gnb_to_telco_distance'] for avg in hop_averages.values() if avg['gnb_to_telco_distance'] > 0) / len([avg for avg in hop_averages.values() if avg['gnb_to_telco_distance'] > 0])
                avg_telco_to_tso_dist = sum(avg['telco_to_tso_distance'] for avg in hop_averages.values() if avg['telco_to_tso_distance'] > 0) / len([avg for avg in hop_averages.values() if avg['telco_to_tso_distance'] > 0])
                
                hop_stats = f"""
HOP-BY-HOP AVERAGE TIMING AND DISTANCES:
- PMU → GNB: {avg_pmu_to_gnb:.4f}s (avg distance: {avg_pmu_to_gnb_dist:.1f}m)
- GNB → TELCO: {avg_gnb_to_telco:.4f}s (avg distance: {avg_gnb_to_telco_dist:.1f}m)  
- TELCO → TSO: {avg_telco_to_tso:.4f}s (avg distance: {avg_telco_to_tso_dist:.1f}m)
- Total Path: {avg_pmu_to_gnb + avg_gnb_to_telco + avg_telco_to_tso:.4f}s (total distance: {avg_pmu_to_gnb_dist + avg_gnb_to_telco_dist + avg_telco_to_tso_dist:.1f}m)
"""
            
            # **NEW: Generate detailed DEADLINE_MISSED statistics per PMU**
            deadline_missed_details = ""
            
            # **FIXED: Calculate overall deadline missed from entire CSV file**
            overall_deadline_stats = calculate_overall_deadline_missed(simulation_folder)
            total_deadline_missed = overall_deadline_stats['deadline_missed']
            total_all_transfers = overall_deadline_stats['total_transfers']
            overall_deadline_missed_rate = overall_deadline_stats['deadline_missed_rate']
            
            # **NEW: Get accurate transfer statistics per PMU and per GNB**
            accurate_stats = calculate_accurate_transfer_stats(simulation_folder)
            accurate_pmu_stats = accurate_stats['pmu_stats']
            accurate_gnb_stats = accurate_stats['gnb_stats']
            
            if hop_averages:
                
                # Find PMUs with deadline missed data
                pmus_with_missed_deadlines = []
                for pmu_id, stats in hop_averages.items():
                    missed_count = stats.get('deadline_missed', 0)
                    total_count = stats.get('total_transfers', 0)
                    if missed_count > 0:
                        missed_rate = (missed_count / total_count) * 100 if total_count > 0 else 0
                        gnb_name = stats.get('gnb_name', 'GNB_Unknown')
                        pmus_with_missed_deadlines.append({
                            'pmu_id': pmu_id,
                            'missed': missed_count,
                            'total': total_count,
                            'rate': missed_rate,
                            'gnb': gnb_name
                        })
                
                # Sort by PMU ID
                pmus_with_missed_deadlines.sort(key=lambda x: x['pmu_id'])
                
                # **Enhanced: Show ALL PMUs in detailed view, not just those with deadline misses**
                deadline_missed_details = f"""
DEADLINE MISSED SUMMARY:
- Total Deadline Missed: {total_deadline_missed}/{total_all_transfers} ({overall_deadline_missed_rate:.2f}%)
- PMUs with Deadline Misses: {len(pmus_with_missed_deadlines)}/{len(hop_averages)}

DETAILED SUCCESSFUL TRANSFERS PER PMU (ALL PMUs):"""
                
                # **NEW: Create list of ALL PMUs with their stats**
                all_pmu_stats = []
                for pmu_id, stats in hop_averages.items():
                    missed_count = stats.get('deadline_missed', 0)
                    total_count = stats.get('total_transfers', 0)
                    missed_rate = (missed_count / total_count) * 100 if total_count > 0 else 0
                    gnb_name = stats.get('gnb_name', 'GNB_Unknown')
                    
                    all_pmu_stats.append({
                        'pmu_id': pmu_id,
                        'missed': missed_count,
                        'total': total_count,
                        'rate': missed_rate,
                        'gnb': gnb_name
                    })
                
                # Sort by PMU ID
                all_pmu_stats.sort(key=lambda x: x['pmu_id'])
                
                # **FIXED: Use accurate PMU stats from CSV**
                if accurate_pmu_stats:
                    for pmu_id in sorted(accurate_pmu_stats.keys()):
                        stats = accurate_pmu_stats[pmu_id]
                        # **Get average time to GNB for this PMU**
                        avg_time_to_gnb = pmu_to_gnb_avg_times.get(pmu_id, 0.0)
                        avg_time_to_tso = stats.get('avg_hop_sum', 0.0)
                        avg_time_str = f" - avg time to GNB: {avg_time_to_gnb:.4f}s, avg time to TSO: {avg_time_to_tso:.4f}s"
                        
                        # **Use accurate counts from CSV**
                        ok_count = stats['ok']
                        total_count = stats['total']
                        success_rate = (ok_count / total_count) * 100 if total_count > 0 else 0
                        gnb_name = stats['gnb_name']
                        
                        # Show successful transfers for all PMUs
                        deadline_missed_details += f"""
  PMU_{pmu_id:02d} → {gnb_name}: {ok_count:2d}/{total_count:2d} transfers on time ({success_rate:5.1f}%){avg_time_str}"""
                else:
                    # Fallback to old method if accurate stats not available
                    for pmu_info in all_pmu_stats:
                        # **Get average time to GNB for this PMU**
                        avg_time_to_gnb = pmu_to_gnb_avg_times.get(pmu_info['pmu_id'], 0.0)
                        avg_time_str = f" - avg time to GNB: {avg_time_to_gnb:.4f}s"
                        
                        # **Calculate successful transfers instead of missed**
                        successful_count = pmu_info['total'] - pmu_info['missed']
                        success_rate = (successful_count / pmu_info['total']) * 100 if pmu_info['total'] > 0 else 0
                        
                        # Show successful transfers for all PMUs
                        deadline_missed_details += f"""
  PMU_{pmu_info['pmu_id']:02d} → {pmu_info['gnb']}: {successful_count:2d}/{pmu_info['total']:2d} transfers on time ({success_rate:5.1f}%){avg_time_str}"""
                
                # **FIXED: Use accurate GNB stats from CSV**
                if accurate_gnb_stats:
                    deadline_missed_details += f"""

SUCCESSFUL TRANSFERS SUMMARY BY GNB:"""
                    for gnb_name in sorted(accurate_gnb_stats.keys()):
                        stats = accurate_gnb_stats[gnb_name]
                        # **Use accurate counts from CSV**  
                        ok_count = stats['ok']
                        total_count = stats['total']
                        success_rate = (ok_count / total_count) * 100 if total_count > 0 else 0
                        
                        # Count PMUs for this GNB and calculate average time to GNB
                        pmu_count = 0
                        gnb_times = []
                        for pmu_id, pmu_stats in accurate_pmu_stats.items():
                            if pmu_stats['gnb_name'] == gnb_name:
                                pmu_count += 1
                                # Get average time to GNB for this PMU
                                avg_time_to_gnb = pmu_to_gnb_avg_times.get(pmu_id, 0.0)
                                if avg_time_to_gnb > 0:
                                    gnb_times.append(avg_time_to_gnb)
                        
                        # Calculate average time to this GNB
                        avg_gnb_time = sum(gnb_times) / len(gnb_times) if gnb_times else 0.0
                        
                        deadline_missed_details += f"""
  {gnb_name}: {ok_count}/{total_count} transfers on time ({success_rate:.1f}%) from {pmu_count} PMUs - avg time to GNB: {avg_gnb_time:.4f}s"""
                else:
                    # Fallback to old method if accurate stats not available
                    gnb_missed_stats = {}
                    for pmu_info in all_pmu_stats:
                        gnb = pmu_info['gnb']
                        if gnb not in gnb_missed_stats:
                            gnb_missed_stats[gnb] = {'missed': 0, 'total': 0, 'pmu_count': 0}
                        gnb_missed_stats[gnb]['missed'] += pmu_info['missed']
                        gnb_missed_stats[gnb]['total'] += pmu_info['total']
                        gnb_missed_stats[gnb]['pmu_count'] += 1
                    
                    if gnb_missed_stats:
                        deadline_missed_details += f"""

SUCCESSFUL TRANSFERS SUMMARY BY GNB:"""
                        for gnb, stats in sorted(gnb_missed_stats.items()):
                            # **Calculate successful transfers instead of missed**
                            successful_count = stats['total'] - stats['missed']
                            success_rate = (successful_count / stats['total']) * 100 if stats['total'] > 0 else 0
                            deadline_missed_details += f"""
  {gnb}: {successful_count}/{stats['total']} transfers on time ({success_rate:.1f}%) from {stats['pmu_count']} PMUs"""
            
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
        if file.endswith("_state_estimation.csv"):
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
            
            # Total time statistics
            avg_total_time = df['TotalTime'].mean()
            min_total_time = df['TotalTime'].min()
            max_total_time = df['TotalTime'].max()
            
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
    
    comprehensive_report += f"""
========================================
Analysis completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
========================================
"""
    
    # **Save single comprehensive statistics file**
    stats_file = os.path.join(simulation_folder, "pmu_simulation_statistics.txt")
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
        
        # **NEW: Get accurate transfer statistics**
        accurate_stats = calculate_accurate_transfer_stats(simulation_folder)
        accurate_pmu_stats = accurate_stats['pmu_stats']
        accurate_gnb_stats = accurate_stats['gnb_stats']
    except Exception as e:
        hop_averages = {}
        pmu_to_gnb_avg_times = {}
        accurate_pmu_stats = {}
        accurate_gnb_stats = {}
        print(f"Could not get hop averages for CSV export: {e}")
    
    # **Create simple CSV data structure - Just Metric and Value**
    csv_data = []
    
    # **PMU DATA SECTION**
    csv_data.append(['=== PMU DATA TRANSFER STATISTICS ===', ''])
    
    # **1. Total Data Volume**
    pmu_csv = None
    for file in os.listdir(simulation_folder):
        if file.endswith("_pmu.csv"):
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
    
    # **2. HOP-BY-HOP AVERAGE TIMING AND DISTANCES**
    csv_data.append(['=== HOP-BY-HOP AVERAGE TIMING AND DISTANCES ===', ''])
    
    if hop_averages:
        # Calculate averages
        avg_pmu_to_gnb = sum(avg['gnb'] for avg in hop_averages.values() if avg['gnb'] > 0) / len([avg for avg in hop_averages.values() if avg['gnb'] > 0])
        avg_gnb_to_telco = sum(avg['gnb_to_telco'] for avg in hop_averages.values() if avg['gnb_to_telco'] > 0) / len([avg for avg in hop_averages.values() if avg['gnb_to_telco'] > 0])
        avg_telco_to_tso = sum(avg['telco_to_tso'] for avg in hop_averages.values() if avg['telco_to_tso'] > 0) / len([avg for avg in hop_averages.values() if avg['telco_to_tso'] > 0])
        
        avg_pmu_to_gnb_dist = sum(avg['gnb_distance'] for avg in hop_averages.values() if avg['gnb_distance'] > 0) / len([avg for avg in hop_averages.values() if avg['gnb_distance'] > 0])
        avg_gnb_to_telco_dist = sum(avg['gnb_to_telco_distance'] for avg in hop_averages.values() if avg['gnb_to_telco_distance'] > 0) / len([avg for avg in hop_averages.values() if avg['gnb_to_telco_distance'] > 0])
        avg_telco_to_tso_dist = sum(avg['telco_to_tso_distance'] for avg in hop_averages.values() if avg['telco_to_tso_distance'] > 0) / len([avg for avg in hop_averages.values() if avg['telco_to_tso_distance'] > 0])
        
        csv_data.append(['PMU → GNB Time', f'{avg_pmu_to_gnb:.4f}s'])
        csv_data.append(['PMU → GNB Distance', f'{avg_pmu_to_gnb_dist:.1f}m'])
        csv_data.append(['GNB → TELCO Time', f'{avg_gnb_to_telco:.4f}s'])
        csv_data.append(['GNB → TELCO Distance', f'{avg_gnb_to_telco_dist:.1f}m'])
        csv_data.append(['TELCO → TSO Time', f'{avg_telco_to_tso:.4f}s'])
        csv_data.append(['TELCO → TSO Distance', f'{avg_telco_to_tso_dist:.1f}m'])
        csv_data.append(['Total Path Time', f'{avg_pmu_to_gnb + avg_gnb_to_telco + avg_telco_to_tso:.4f}s'])
        csv_data.append(['Total Path Distance', f'{avg_pmu_to_gnb_dist + avg_gnb_to_telco_dist + avg_telco_to_tso_dist:.1f}m'])
    
    csv_data.append(['', ''])  # Empty row
    
    # **3. DEADLINE MISSED SUMMARY**
    csv_data.append(['=== DEADLINE MISSED SUMMARY ===', ''])
    
    # **FIXED: Use correct overall deadline missed calculation from entire CSV**
    overall_deadline_stats = calculate_overall_deadline_missed(simulation_folder)
    total_deadline_missed = overall_deadline_stats['deadline_missed']
    total_all_transfers = overall_deadline_stats['total_transfers']
    overall_deadline_missed_rate = overall_deadline_stats['deadline_missed_rate']
    
    csv_data.append(['Total Deadline Missed', f'{total_deadline_missed}/{total_all_transfers}'])
    csv_data.append(['Deadline Missed Rate', f'{overall_deadline_missed_rate:.2f}%'])
    
    if hop_averages:
        pmus_with_misses = len([avg for avg in hop_averages.values() if avg.get('deadline_missed', 0) > 0])
        csv_data.append(['PMUs with Deadline Misses', f'{pmus_with_misses}/{len(hop_averages)}'])
    
    csv_data.append(['', ''])  # Empty row
    
    # **4. DETAILED SUCCESSFUL TRANSFERS PER PMU**
    csv_data.append(['=== DETAILED SUCCESSFUL TRANSFERS PER PMU ===', ''])
    
    # **FIXED: Use accurate PMU stats from CSV**
    if accurate_pmu_stats:
        for pmu_id in sorted(accurate_pmu_stats.keys()):
            stats = accurate_pmu_stats[pmu_id]
            ok_count = stats['ok']
            total_count = stats['total']
            success_rate = (ok_count / total_count) * 100 if total_count > 0 else 0
            gnb_name = stats['gnb_name']
            avg_time_to_gnb = pmu_to_gnb_avg_times.get(pmu_id, 0.0)
            avg_time_to_tso = stats.get('avg_hop_sum', 0.0)
            
            # Simple format with only numbers
            pmu_detail = f'{ok_count}/{total_count} ({success_rate:.1f}%) - GNB: {avg_time_to_gnb:.4f}s, TSO: {avg_time_to_tso:.4f}s'
            csv_data.append([f'PMU_{pmu_id:02d} → {gnb_name}', pmu_detail])
    elif hop_averages:
        # Fallback to old method if accurate stats not available
        for pmu_id, stats in sorted(hop_averages.items()):
            missed_count = stats.get('deadline_missed', 0)
            total_count = stats.get('total_transfers', 0)
            successful_count = total_count - missed_count  # Calculate successful
            success_rate = (successful_count / total_count) * 100 if total_count > 0 else 0
            gnb_name = stats.get('gnb_name', 'GNB_Unknown')
            avg_time_to_gnb = pmu_to_gnb_avg_times.get(pmu_id, 0.0)
            
            # Simple format with only numbers
            pmu_detail = f'{successful_count}/{total_count} ({success_rate:.1f}%) - {avg_time_to_gnb:.4f}s'
            csv_data.append([f'PMU_{pmu_id:02d} → {gnb_name}', pmu_detail])
    
    csv_data.append(['', ''])  # Empty row
    
    # **5. SUCCESSFUL TRANSFERS SUMMARY BY GNB**
    csv_data.append(['=== SUCCESSFUL TRANSFERS SUMMARY BY GNB ===', ''])
    
    # **FIXED: Use accurate GNB stats from CSV**
    if accurate_gnb_stats:
        for gnb_name in sorted(accurate_gnb_stats.keys()):
            stats = accurate_gnb_stats[gnb_name]
            ok_count = stats['ok']
            total_count = stats['total']
            success_rate = (ok_count / total_count) * 100 if total_count > 0 else 0
            
            # Count PMUs for this GNB and calculate average time to GNB
            pmu_count = 0
            gnb_times = []
            for pmu_id, pmu_stats in accurate_pmu_stats.items():
                if pmu_stats['gnb_name'] == gnb_name:
                    pmu_count += 1
                    # Get average time to GNB for this PMU
                    avg_time_to_gnb = pmu_to_gnb_avg_times.get(pmu_id, 0.0)
                    if avg_time_to_gnb > 0:
                        gnb_times.append(avg_time_to_gnb)
            
            # Calculate average time to this GNB
            avg_gnb_time = sum(gnb_times) / len(gnb_times) if gnb_times else 0.0
            
            # Simple format with only numbers
            gnb_detail = f"{ok_count}/{total_count} ({success_rate:.1f}%) from {pmu_count} PMUs - avg time: {avg_gnb_time:.4f}s"
            csv_data.append([gnb_name, gnb_detail])
    elif hop_averages:
        # Fallback to old method if accurate stats not available
        gnb_missed_stats = {}
        for pmu_id, stats in hop_averages.items():
            gnb = stats.get('gnb_name', 'GNB_Unknown')
            if gnb not in gnb_missed_stats:
                gnb_missed_stats[gnb] = {'missed': 0, 'total': 0, 'pmu_count': 0}
            gnb_missed_stats[gnb]['missed'] += stats.get('deadline_missed', 0)
            gnb_missed_stats[gnb]['total'] += stats.get('total_transfers', 0)
            gnb_missed_stats[gnb]['pmu_count'] += 1
        
        for gnb, stats in sorted(gnb_missed_stats.items()):
            successful_count = stats['total'] - stats['missed']  # Calculate successful
            success_rate = (successful_count / stats['total']) * 100 if stats['total'] > 0 else 0
            # Simple format with only numbers
            gnb_detail = f"{successful_count}/{stats['total']} ({success_rate:.1f}%) from {stats['pmu_count']} PMUs"
            csv_data.append([gnb, gnb_detail])
    
    csv_data.append(['', ''])  # Empty row
    csv_data.append(['', ''])  # Extra separation
    
    # **GRID ANALYSIS SECTION**
    csv_data.append(['=== GRID ANALYSIS TASK STATISTICS ===', ''])
    
    # **6. GRID ANALYSIS TASK COMPLETION**
    state_csv = None
    for file in os.listdir(simulation_folder):
        if file.endswith("_state_estimation.csv"):
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
        
        return csv_file
        
    except Exception as e:
        print(f"❌ Error writing CSV file: {e}")
        logger.error(f"Error writing CSV file: {e}")
        return None

def create_performance_charts(simulation_folder: str, logger: logging.Logger):
    """Create performance analysis charts with 4 subplots showing PMU and TSO statistics."""
    if not SHOW_PLOTS:
        return
    
    logger.info("Creating Performance Analysis Charts...")
    
    try:
        # Get accurate transfer statistics
        accurate_stats = calculate_accurate_transfer_stats(simulation_folder)
        accurate_pmu_stats = accurate_stats['pmu_stats']
        accurate_gnb_stats = accurate_stats['gnb_stats']
        
        # Get PMU to GNB average times
        pmu_to_gnb_avg_times = calculate_pmu_to_gnb_average_times(simulation_folder)
        
        # Get GNB network times from HopSum values
        gnb_network_times = calculate_gnb_network_times(simulation_folder)
        
        if not accurate_pmu_stats or not accurate_gnb_stats:
            logger.warning("No data available for performance charts")
            return
        
        # Create figure with 2x2 subplots
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('PMU Smart Grid Performance Analysis', fontsize=16, fontweight='bold')
        
        # **Chart 1: PMU Success Rate (πάνω αριστερά - πράσινο)**
        pmu_ids = sorted(accurate_pmu_stats.keys())
        pmu_success_rates = []
        pmu_labels = []
        
        for pmu_id in pmu_ids:
            stats = accurate_pmu_stats[pmu_id]
            ok_count = stats['ok']
            total_count = stats['total']
            success_rate = (ok_count / total_count) * 100 if total_count > 0 else 0
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
        
        # **Chart 2: PMU Average Transfer Delay (πάνω δεξιά - μπλε)**
        pmu_avg_delays = []
        pmu_delay_labels = []
        
        for pmu_id in pmu_ids:
            stats = accurate_pmu_stats[pmu_id]
            avg_delay = stats.get('avg_hop_sum', 0.0)
            pmu_avg_delays.append(avg_delay)
            pmu_delay_labels.append(f'{pmu_id:02d}')
        
        bars2 = ax2.bar(pmu_delay_labels, pmu_avg_delays, color='blue')
        ax2.set_title('PMU Average Transfer Delay', fontweight='bold')
        ax2.set_xlabel('PMU ID')
        ax2.set_ylabel('Average Transfer Delay (ms)')
        ax2.grid(True, alpha=0.3)
        
        # Add value labels on bars (convert to ms)
        for bar, delay in zip(bars2, pmu_avg_delays):
            height = bar.get_height()
            delay_ms = delay * 1000  # Convert to milliseconds
            ax2.text(bar.get_x() + bar.get_width()/2., height + height*0.02,
                    f'{delay_ms:.0f}ms', ha='center', va='bottom', fontsize=8)
        
        # Rotate x-axis labels if too many PMUs
        if len(pmu_ids) > 10:
            ax2.tick_params(axis='x', rotation=45)
        
        # **Chart 3: GNB Success Rate (κάτω αριστερά - κόκκινο)**
        gnb_names = sorted(accurate_gnb_stats.keys())
        gnb_success_rates = []
        
        for gnb_name in gnb_names:
            stats = accurate_gnb_stats[gnb_name]
            ok_count = stats['ok']
            total_count = stats['total']
            success_rate = (ok_count / total_count) * 100 if total_count > 0 else 0
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
        
        # **Chart 4: GNB Average Timings (κάτω δεξιά - Stacked bar chart)**
        # Read state estimation CSV to get TSO execution times
        state_csv = None
        for file in os.listdir(simulation_folder):
            if file.endswith("_state_estimation.csv") or file == "Sequential_simulation_state_estimation.csv":
                state_csv = os.path.join(simulation_folder, file)
                break
        
        if state_csv and os.path.exists(state_csv) and gnb_network_times:
            import pandas as pd
            df_state = pd.read_csv(state_csv)
            
            # **For cloud scenario, all processing happens at TSO**
            # Calculate average times for TSO processing
            avg_exec_time = df_state['ExecTime'].mean() if 'ExecTime' in df_state.columns else 0.0
            avg_pdc_waiting = df_state['PDCWaitingTime'].mean() if 'PDCWaitingTime' in df_state.columns else 0.0
            
            # Get GNB names and their network times
            gnb_names = sorted(gnb_network_times.keys())
            
            if gnb_names:
                # Prepare data for stacked bar chart
                network_times = []
                return_network_times = []  # Return network time (same as network time)
                pdc_times = []
                exec_times = []
                
                for gnb_name in gnb_names:
                    network_time = gnb_network_times[gnb_name]
                    network_times.append(network_time)
                    return_network_times.append(network_time)  # Return time = network time
                    pdc_times.append(avg_pdc_waiting)
                    exec_times.append(avg_exec_time)
                
                # Create stacked bar chart for each GNB
                # Stacking order: Network Time → PDC Waiting Time → Execution Time → Return Network Time
                bars4_network = ax4.bar(gnb_names, network_times, color='blue', label='Network Time', width=0.6)
                bars4_pdc = ax4.bar(gnb_names, pdc_times, 
                                   bottom=network_times, 
                                   color='lightcoral', label='PDC Waiting Time', width=0.6)
                bars4_exec = ax4.bar(gnb_names, exec_times, 
                                   bottom=[n + p for n, p in zip(network_times, pdc_times)], 
                                   color='orange', label='Execution Time', width=0.6)
                bars4_return = ax4.bar(gnb_names, return_network_times, 
                                      bottom=[n + p + e for n, p, e in zip(network_times, pdc_times, exec_times)], 
                                      color='lightblue', label='Return Network Time', width=0.6)
                
                ax4.set_title('GNB Average Timings', fontweight='bold')
                ax4.set_xlabel('GNB ID')
                ax4.set_ylabel('Average Time (ms)')
                ax4.grid(True, alpha=0.3)
                ax4.legend(loc='upper right', fontsize=8)
                
                # Add total time labels on top of bars (convert to ms)
                for i, gnb_name in enumerate(gnb_names):
                    total_time = network_times[i] + pdc_times[i] + exec_times[i] + return_network_times[i]
                    total_time_ms = total_time * 1000  # Convert to milliseconds
                    ax4.text(i, total_time + total_time*0.02,
                            f'{total_time_ms:.0f}ms', ha='center', va='bottom', fontsize=9, fontweight='bold')
                
                # Rotate x-axis labels if too many GNBs
                if len(gnb_names) > 6:
                    ax4.tick_params(axis='x', rotation=45)
            else:
                # No GNB data - show empty chart
                ax4.text(0.5, 0.5, 'No GNB Network Data Available', ha='center', va='center', 
                        transform=ax4.transAxes, fontsize=12, color='gray')
                ax4.set_title('GNB Average Timings', fontweight='bold')
                ax4.set_xlabel('GNB ID')
                ax4.set_ylabel('Average Time (s)')
        else:
            # No state estimation data - show empty chart
            ax4.text(0.5, 0.5, 'No TSO Data Available', ha='center', va='center', 
                    transform=ax4.transAxes, fontsize=12, color='gray')
            ax4.set_title('GNB Average Timings', fontweight='bold')
            ax4.set_xlabel('GNB ID')
            ax4.set_ylabel('Average Time (s)')
        
        # Adjust layout and save
        plt.tight_layout()
        plt.subplots_adjust(top=0.93)  # Make room for suptitle
        
        # Save the charts
        output_path = os.path.join(simulation_folder, "performance_analysis_charts.png")
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Performance analysis charts saved at: {output_path}")
        print(f"📊 Performance analysis charts created: {output_path}")
        
    except Exception as e:
        logger.error(f"Error creating performance charts: {str(e)}")
        print(f"❌ Error creating performance charts: {e}")

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