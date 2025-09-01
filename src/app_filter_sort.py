from flask import Flask, request
from datetime import datetime as dt_parser


def filter_priority(blitz_records, max_priority):
    blitz_records = [record for record in blitz_records
            if record.priority is not None and record.priority >= 0]
    if max_priority:
        try:
            max_priority_int = int(max_priority)
            blitz_records = [record for record in blitz_records
                           if record.priority is not None and record.priority <= max_priority_int]
        except ValueError:
            pass  # Invalid priority value, ignore filter
    return blitz_records


def filter_blitz(blitz_records):
    all_groups = set()
    for record in blitz_records:
        if record.finding:
            all_groups.add(record.finding)

    finding_groups = sorted(list(all_groups))

        # Get selected finding groups from request
    selected_finding_groups = request.args.getlist('finding_groups')

        # Check if this is an explicit "deselect all" action
    deselect_all = request.args.get('deselect_all') == 'true'

    if deselect_all:
            # Explicitly show no results
        selected_finding_groups = []
        blitz_records = []
    elif not selected_finding_groups:
            # If no groups selected and not explicit deselect, show all groups (default behavior)
        selected_finding_groups = finding_groups
    else:
            # Filter records by selected finding groups
        blitz_records = [record for record in blitz_records
                           if record.finding and record.finding in selected_finding_groups]

    return blitz_records

def filter_blitz_index(blitz_records):
    all_groups = set()
    for record in blitz_records:
        if record.finding:
            if ':' in record.finding:
                group = record.finding.split(':', 1)[0]
                all_groups.add(group)

    finding_groups = sorted(list(all_groups))

        # Get selected finding groups from request
    selected_finding_groups = request.args.getlist('finding_groups')

        # Check if this is an explicit "deselect all" action
    deselect_all = request.args.get('deselect_all') == 'true'

    if deselect_all:
            # Explicitly show no results
        selected_finding_groups = []
        blitz_records = []
    elif not selected_finding_groups:
            # If no groups selected and not explicit deselect, show all groups (default behavior)
        selected_finding_groups = finding_groups
    else:
            # Filter records by selected finding groups
        filtered_records = []
        for record in blitz_records:
            if record.finding and ':' in record.finding:
                group = record.finding.split(':', 1)[0]
                if group in selected_finding_groups:
                    filtered_records.append(record)
        blitz_records = filtered_records
    return blitz_records,finding_groups,selected_finding_groups

def filter_blitz_cache(blitz_records, min_avg_cpu, min_total_cpu, min_executions, min_total_reads):

    # Apply filters if they are provided and valid
    if min_avg_cpu:
        try:
            min_avg_cpu_float = float(min_avg_cpu)
            blitz_records = [record for record in blitz_records
                            if record.avg_cpu_ms is not None and record.avg_cpu_ms >= min_avg_cpu_float]
        except ValueError:
            pass  # Invalid value, ignore filter

    if min_total_cpu:
        try:
            min_total_cpu_float = float(min_total_cpu)
            blitz_records = [record for record in blitz_records
                            if record.total_cpu_ms is not None and record.total_cpu_ms >= min_total_cpu_float]
        except ValueError:
            pass  # Invalid value, ignore filter

    if min_executions:
        try:
            min_executions_int = int(min_executions)
            blitz_records = [record for record in blitz_records
                            if record.executions is not None and record.executions >= min_executions_int]
        except ValueError:
            pass  # Invalid value, ignore filter

    if min_total_reads:
        try:
            min_total_reads_int = int(min_total_reads)
            blitz_records = [record for record in blitz_records
                            if record.total_reads is not None and record.total_reads >= min_total_reads_int]
        except ValueError:
            pass  # Invalid value, ignore filter

    return blitz_records

def sort_records(blitz_records, sort_by, sort_order):
    if sort_by in ['avg_cpu_ms', 'total_cpu_ms', 'executions', 'total_reads']:
        reverse = sort_order == 'desc'
            # Sort records, handling None values by placing them at the end
        blitz_records = sorted(blitz_records,
                                 key=lambda x: getattr(x, sort_by) or 0,
                                 reverse=reverse)

    return blitz_records

def filter_by_hour(start_hour, end_hour, blitz_records):
    if start_hour and end_hour:
        try:
            start_hour_int = int(start_hour)
            end_hour_int = int(end_hour)
            print(f"Filtering records between hours: {start_hour_int} and {end_hour_int}")

            # Filter records based on hour of last_execution
            filtered_records = []
            for record in blitz_records:
                if record.last_execution:
                    # Extract hour from last_execution datetime
                    try:
                        if hasattr(record.last_execution, 'hour'):
                            # It's a datetime object
                            execution_hour = record.last_execution.hour
                        else:
                            # It's a string, try to parse it
                            execution_hour = None
                            for fmt in ['%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', '%m/%d/%Y %H:%M:%S']:
                                try:
                                    dt = dt_parser.strptime(str(record.last_execution), fmt)
                                    execution_hour = dt.hour
                                    break
                                except ValueError:
                                    continue

                            if execution_hour is None:
                                continue

                        # Check if execution hour is within the selected range
                        if start_hour_int <= end_hour_int:
                            # Normal range (e.g., 9-17)
                            if start_hour_int <= execution_hour <= end_hour_int:
                                filtered_records.append(record)
                        else:
                            # Range crosses midnight (e.g., 22-6)
                            if execution_hour >= start_hour_int or execution_hour <= end_hour_int:
                                filtered_records.append(record)
                    except (AttributeError, ValueError):
                        continue

            return filtered_records
        except ValueError:
            return blitz_records
