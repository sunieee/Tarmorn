#!/usr/bin/env python3
"""
Merge multiple part_*.json files into a single atom2formula2metric.json and rule.txt
"""

import json
import os
import sys
import glob
import re
from pathlib import Path
from collections import defaultdict


def parse_atom_to_rule_string(atom_str):
    """
    Parse atom string like "r123(X,Y)" or "INVERSE_r456(X,c789)" 
    and convert to rule string format using getAtomString logic.
    
    For path length == 1: relation(X, entity)
    For path length > 1: r1(X,A), r2(A,B), ..., rN(prev, entity)
    Handle INVERSE relations by swapping arguments
    Special: if entity is '·', use the previous variable name instead
    """
    # Extract relation path and entity
    if '(' not in atom_str:
        return atom_str
    
    # Parse: relation_part(X, entity_part)
    match = re.match(r'(.+?)\(X,(.+?)\)$', atom_str)
    if not match:
        return atom_str
    
    relation_part = match.group(1)
    entity_part = match.group(2)
    
    # Split relation path by '·' (middle dot)
    relations = relation_part.split('·')
    n = len(relations)
    
    # Build node list: X, A, B, ..., tailTerm
    # Start with intermediate variables
    nodes = ['X']
    for i in range(n):
        nodes.append(chr(ord('A') + i))
    
    # Handle terminal entity:
    # If entity is '·', keep the last variable (don't replace nodes[n])
    # Otherwise, replace nodes[n] with the actual entity
    tail_term = entity_part
    if tail_term != '·':
        nodes[n] = tail_term
    
    # Build rule string parts
    parts = []
    for i in range(n):
        r = relations[i]
        is_inverse = r.startswith('INVERSE_')
        
        # Get forward relation name
        forward_name = r[8:] if is_inverse else r  # Remove 'INVERSE_' prefix
        
        # Determine arguments (swap for inverse)
        if is_inverse:
            left = nodes[i + 1]
            right = nodes[i]
        else:
            left = nodes[i]
            right = nodes[i + 1]
        
        parts.append(f"{forward_name}({left},{right})")
    
    return ', '.join(parts)


def parse_formula_to_rule_string(formula_str):
    """
    Parse formula (comma-separated atoms) and convert each atom to rule string
    """
    if not formula_str or formula_str.strip() == '':
        return ''
    
    # Split by comma but handle nested commas in atom strings
    atoms = []
    current_atom = ''
    paren_depth = 0
    
    for char in formula_str:
        if char == '(':
            paren_depth += 1
            current_atom += char
        elif char == ')':
            paren_depth -= 1
            current_atom += char
        elif char == ',' and paren_depth == 0:
            if current_atom.strip():
                atoms.append(current_atom.strip())
            current_atom = ''
        else:
            current_atom += char
    
    if current_atom.strip():
        atoms.append(current_atom.strip())
    
    # Convert each atom to rule string
    rule_atoms = [parse_atom_to_rule_string(atom) for atom in atoms]
    return ', '.join(rule_atoms)


def merge_metrics(metrics_list):
    """
    Merge multiple metrics by:
    - support, headSize, bodySize: sum
    - jaccard: average
    - confidence: recalculate as support/bodySize
    - size: count of merged metrics
    """
    total_support = 0
    total_head_size = 0
    total_body_size = 0
    total_jaccard = 0
    count = len(metrics_list)
    
    for metric in metrics_list:
        total_support += metric['support']
        total_head_size += metric['headSize']
        total_body_size += metric['bodySize']
        total_jaccard += metric['jaccard']
    
    # Calculate average jaccard
    avg_jaccard = total_jaccard / count if count > 0 else 0
    
    # Recalculate confidence
    confidence = total_support / total_body_size if total_body_size > 0 else 0
    
    return {
        'jaccard': avg_jaccard,
        'support': total_support,
        'headSize': total_head_size,
        'bodySize': total_body_size,
        'confidence': confidence,
        'size': count
    }


def merge_json_files(input_dirs):
    """
    Merge all part_*.json files from multiple input directories
    
    Args:
        input_dirs: list of directory paths
    """
    # Collect all part_*.json files from all directories
    all_json_files = []
    for input_dir in input_dirs:
        json_files = sorted(glob.glob(os.path.join(input_dir, 'part_*.json')))
        if json_files:
            print(f"\nFound {len(json_files)} files in {input_dir}:")
            for f in json_files:
                print(f"  - {os.path.basename(f)}")
            all_json_files.extend(json_files)
        else:
            print(f"\nWarning: No part_*.json files found in {input_dir}")
    
    if not all_json_files:
        print("Error: No part_*.json files found in any directory")
        return None
    
    print(f"\nTotal files to merge: {len(all_json_files)}")
    
    # Merged structure: atom -> formula -> list of metrics
    merged = defaultdict(lambda: defaultdict(list))
    
    # Read and merge all JSON files
    for json_file in all_json_files:
        print(f"\nProcessing {json_file}...")
        
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Iterate through atoms
        for atom, formulas in data.items():
            for formula, metric in formulas.items():
                # Append metric to the list
                merged[atom][formula].append(metric)
        
        print(f"  Loaded {len(data)} atoms")
    
    # Merge metrics for each atom-formula pair
    print("\nMerging metrics...")
    final_merged = {}
    
    for atom, formulas in merged.items():
        final_merged[atom] = {}
        for formula, metrics_list in formulas.items():
            # Merge all metrics for this atom-formula pair
            final_merged[atom][formula] = merge_metrics(metrics_list)
    
    print(f"Merged result: {len(final_merged)} unique atoms")
    
    return final_merged


def write_json_output(merged_data, output_file):
    """
    Write merged data to JSON file
    """
    print(f"\nWriting JSON to {output_file}...")
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(merged_data, f, indent=2, ensure_ascii=False)
    
    print(f"Successfully saved to {output_file}")


def write_rules_output(merged_data, output_file):
    """
    Write rules to text file in format:
    bodySize\tsupport\tconfidence\tatom_rule_string <= formula_rule_string
    """
    print(f"\nWriting rules to {output_file}...")
    
    total_rules = 0
    
    with open(output_file, 'w', encoding='utf-8') as f:
        # Sort atoms for consistent output
        for atom in sorted(merged_data.keys()):
            formulas = merged_data[atom]
            
            # Sort formulas by confidence (descending)
            sorted_formulas = sorted(
                formulas.items(),
                key=lambda x: x[1]['confidence'],
                reverse=True
            )
            
            for formula, metric in sorted_formulas:
                body_size = metric['bodySize']
                support = int(metric['support'])
                confidence = metric['confidence']
                
                # Convert atom and formula to rule string format
                atom_rule_string = parse_atom_to_rule_string(atom)
                formula_rule_string = parse_formula_to_rule_string(formula)
                
                # Format: bodySize\tsupport\tconfidence\thead <= body
                rule_line = f"{body_size}\t{support}\t{confidence}\t{atom_rule_string} <= {formula_rule_string}\n"
                f.write(rule_line)
                total_rules += 1
    
    print(f"Successfully saved {total_rules} rules to {output_file}")
    return total_rules


def print_statistics(merged_data):
    """
    Print statistics about the merged rules
    """
    total_rules = 0
    unary_stats = defaultdict(int)
    binary_stats = defaultdict(int)
    
    for atom, formulas in merged_data.items():
        # Check if atom is binary (contains "(X,Y)")
        is_binary = "(X,Y)" in atom
        
        for formula, metric in formulas.items():
            total_rules += 1
            
            # Count body length (number of atoms in formula)
            # Need to properly count atoms considering nested parentheses
            if not formula or formula.strip() == '':
                body_length = 0
            else:
                # Count by splitting and handling nested structures
                body_length = len([a for a in re.split(r',\s*(?![^()]*\))', formula) if a.strip()])
            
            if is_binary:
                binary_stats[body_length] += 1
            else:
                unary_stats[body_length] += 1
    
    print("\n" + "=" * 60)
    print(f"Total rules: {total_rules}")
    print("=" * 60)
    print("Type     L0       L1       L2       L3")
    print("-" * 60)
    
    # Print unary statistics
    unary_line = "Unary    "
    for i in range(4):
        unary_line += f"{str(unary_stats.get(i, 0)).rjust(8)}  "
    print(unary_line)
    
    # Print binary statistics
    binary_line = "Binary   "
    for i in range(4):
        binary_line += f"{str(binary_stats.get(i, 0)).rjust(8)}  "
    print(binary_line)
    
    print("=" * 60)


def main():
    if len(sys.argv) != 2:
        print("Usage: python merge_rules.py <split_directory>")
        print("Example 1: python merge_rules.py out/FB15k-237/louvain")
        print("Example 2: python merge_rules.py out/FB15k-237/louvain+edge_cut")
        sys.exit(1)
    
    split_dir = sys.argv[1]
    
    if not os.path.isdir(split_dir):
        print(f"Error: {split_dir} is not a valid directory")
        sys.exit(1)
    
    split_name = os.path.basename(split_dir)
    
    # Check if split name contains '+' (combined datasets)
    if '+' in split_name:
        # Split into individual dataset names
        split_names = split_name.split('+')
        
        # Construct input directories for each split
        parent_dir = os.path.dirname(split_dir)
        input_dirs = []
        for name in split_names:
            atom_dir = os.path.join(parent_dir, name, 'atom2formula2metric')
            if not os.path.isdir(atom_dir):
                print(f"Error: {atom_dir} is not a valid directory")
                sys.exit(1)
            input_dirs.append(atom_dir)
        # Output to the combined directory
        output_dir = split_dir
    else:
        # Single dataset: just merge part_*.json in the atom2formula2metric subdirectory
        atom_dir = os.path.join(split_dir, 'atom2formula2metric')
        if not os.path.isdir(atom_dir):
            print(f"Error: {atom_dir} is not a valid directory")
            sys.exit(1)
        input_dirs = [atom_dir]
        output_dir = split_dir
    
    os.makedirs(output_dir, exist_ok=True)
    output_json = os.path.join(output_dir, 'atom2formula2metric.json')
    output_rules = os.path.join(output_dir, 'rule.txt')
    
    print("=" * 60)
    print("Merging JSON files")
    print("=" * 60)
    print(f"Input directories: {len(input_dirs)}")
    for i, dir_path in enumerate(input_dirs, 1):
        print(f"  {i}. {dir_path}")
    print(f"Output directory: {output_dir}")
    print(f"Output JSON: {output_json}")
    print(f"Output rules: {output_rules}")
    print("=" * 60)
    
    # Merge JSON files
    merged_data = merge_json_files(input_dirs)
    
    if merged_data is None:
        sys.exit(1)
    
    # Write outputs
    write_json_output(merged_data, output_json)
    write_rules_output(merged_data, output_rules)
    
    # Print statistics
    print_statistics(merged_data)
    
    print("\nMerge completed successfully!")


if __name__ == '__main__':
    main()
