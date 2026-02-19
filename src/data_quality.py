import pandas as pd
import numpy as np
from datetime import datetime
from collections import defaultdict


def load_data(filepath):
    """Load CSV file into pandas DataFrame."""
    df = pd.read_csv(filepath)
    # Strip whitespace from column names
    df.columns = df.columns.str.strip()
    return df


def analyze_completeness(df):
    """Calculate completeness percentage for each column."""
    completeness = {}
    total_rows = len(df)
    
    for col in df.columns:
        non_null_count = df[col].notna().sum()
        # Also check for empty strings
        empty_strings = (df[col] == '').sum()
        valid_count = non_null_count - empty_strings
        percentage = (valid_count / total_rows) * 100
        completeness[col] = {
            'percentage': round(percentage, 1),
            'missing': total_rows - valid_count
        }
    
    return completeness


def analyze_data_types(df):
    """Analyze data types and detect issues."""
    dtypes = {}
    
    for col in df.columns:
        detected_type = str(df[col].dtype)
        
        # Check if column should be numeric
        if col == 'customer_id':
            expected = 'INT'
            is_valid = detected_type in ['int64', 'float64']
        elif col == 'income':
            expected = 'NUMERIC'
            is_valid = detected_type in ['int64', 'float64'] or df[col].dtype == 'object'
        elif col in ['date_of_birth', 'created_date']:
            expected = 'DATE'
            is_valid = detected_type == 'datetime64[ns]'
            if not is_valid:
                is_valid = 'object'  # Will need conversion
        else:
            expected = 'STRING'
            is_valid = detected_type == 'object'
        
        dtypes[col] = {
            'detected': detected_type,
            'expected': expected,
            'valid': '✓' if is_valid else 'X'
        }
    
    return dtypes


def analyze_format_issues(df):
    """Analyze format inconsistencies in phone numbers and dates."""
    issues = defaultdict(list)
    
    # Phone format analysis
    phone_patterns = df['phone'].astype(str).tolist()
    formats_found = set()
    for phone in phone_patterns:
        if pd.notna(phone):
            if '555-' in phone and phone.count('-') == 2:
                formats_found.add('XXX-XXX-XXXX')
            elif '(' in phone and ')' in phone:
                formats_found.add('(XXX) XXX-XXXX')
            elif '.' in phone:
                formats_found.add('XXX.XXX.XXXX')
            elif phone.isdigit() and len(phone) == 10:
                formats_found.add('XXXXXXXXXX')
            else:
                formats_found.add('OTHER')
    
    issues['phone_formats'] = list(formats_found)
    
    # Date format analysis
    dob_issues = []
    for idx, date_val in enumerate(df['date_of_birth']):
        if pd.notna(date_val) and date_val != '':
            date_str = str(date_val).strip()
            if date_str == 'invalid_date':
                dob_issues.append(f'Row {idx+2}: invalid_date')
            elif '/' in date_str:
                dob_issues.append(f'Row {idx+2}: {date_str} (YYYY/MM/DD format)')
    
    created_date_issues = []
    for idx, date_val in enumerate(df['created_date']):
        if pd.notna(date_val) and date_val != '':
            date_str = str(date_val).strip()
            if date_str == 'invalid_date':
                created_date_issues.append(f'Row {idx+2}: invalid_date')
            elif '/' in date_str and date_str[0].isdigit():
                created_date_issues.append(f'Row {idx+2}: {date_str} (MM/DD/YYYY format)')
    
    issues['date_of_birth_issues'] = dob_issues
    issues['created_date_issues'] = created_date_issues
    
    return issues


def analyze_uniqueness(df):
    """Check uniqueness of customer_id."""
    total_ids = len(df['customer_id'])
    unique_ids = df['customer_id'].nunique()
    duplicates = total_ids - unique_ids
    
    return {
        'total': total_ids,
        'unique': unique_ids,
        'duplicates': duplicates
    }


def analyze_invalid_values(df):
    """Identify invalid values in the dataset."""
    invalid_issues = []
    
    # Check for invalid dates
    for idx, date_val in enumerate(df['date_of_birth']):
        if pd.notna(date_val) and str(date_val).strip() == 'invalid_date':
            invalid_issues.append(f"Row {idx+2}: date_of_birth = 'invalid_date'")
    
    for idx, date_val in enumerate(df['created_date']):
        if pd.notna(date_val) and str(date_val).strip() == 'invalid_date':
            invalid_issues.append(f"Row {idx+2}: created_date = 'invalid_date'")
    
    # Check for negative income
    for idx, income in enumerate(df['income']):
        if pd.notna(income):
            try:
                income_val = float(income)
                if income_val < 0:
                    invalid_issues.append(f"Row {idx+2}: income = {income_val} (negative)")
            except:
                pass
    
    # Check for unrealistic ages (if we can parse the date)
    current_year = datetime.now().year
    for idx, date_val in enumerate(df['date_of_birth']):
        if pd.notna(date_val) and str(date_val).strip() not in ['', 'invalid_date']:
            try:
                if '-' in str(date_val):
                    year = int(str(date_val).split('-')[0])
                    age = current_year - year
                    if age > 150:
                        invalid_issues.append(f"Row {idx+2}: age = {age} (unrealistic)")
            except:
                pass
    
    return invalid_issues


def analyze_categorical_validity(df):
    """Check if account_status contains only valid values."""
    valid_statuses = {'active', 'inactive', 'suspended'}
    issues = []
    
    for idx, status in enumerate(df['account_status']):
        if pd.notna(status) and str(status).strip() != '':
            status_clean = str(status).strip()
            if status_clean not in valid_statuses:
                issues.append(f"Row {idx+2}: '{status}' (invalid value)")
        elif pd.isna(status) or str(status).strip() == '':
            issues.append(f"Row {idx+2}: Empty (missing value)")
    
    return issues


def generate_report(df, completeness, dtypes, format_issues, uniqueness, invalid_values, categorical_issues):
    """Generate the data quality report."""
    report = []
    report.append("DATA QUALITY PROFILE REPORT")
    report.append("=" * 50)
    report.append("")
    
    # Completeness section
    report.append("COMPLETENESS:")
    for col, stats in completeness.items():
        missing_info = f" ({stats['missing']} missing)" if stats['missing'] > 0 else ""
        report.append(f"- {col}: {stats['percentage']}%{missing_info}")
    report.append("")
    
    # Data types section
    report.append("DATA TYPES:")
    for col, info in dtypes.items():
        report.append(f"- {col}: {info['detected']} {info['valid']} (expected: {info['expected']})")
    report.append("")
    
    # Quality issues section
    report.append("QUALITY ISSUES:")
    report.append("1. Format Inconsistencies:")
    report.append(f"   - Phone formats found: {', '.join(format_issues['phone_formats'])}")
    if format_issues['date_of_birth_issues']:
        report.append("   - Date of birth issues:")
        for issue in format_issues['date_of_birth_issues']:
            report.append(f"     * {issue}")
    if format_issues['created_date_issues']:
        report.append("   - Created date issues:")
        for issue in format_issues['created_date_issues']:
            report.append(f"     * {issue}")
    
    report.append("")
    report.append("2. Uniqueness Issues:")
    report.append(f"   - customer_id: {uniqueness['unique']}/{uniqueness['total']} unique")
    if uniqueness['duplicates'] > 0:
        report.append(f"   - Duplicate IDs found: {uniqueness['duplicates']}")
    else:
        report.append("   - All IDs are unique ✓")
    
    report.append("")
    report.append("3. Invalid Values:")
    if invalid_values:
        for issue in invalid_values:
            report.append(f"   - {issue}")
    else:
        report.append("   - None found ✓")
    
    report.append("")
    report.append("4. Categorical Validity (account_status):")
    if categorical_issues:
        for issue in categorical_issues:
            report.append(f"   - {issue}")
    else:
        report.append("   - All values valid ✓")
    
    # Severity assessment
    report.append("")
    report.append("SEVERITY:")
    
    critical = 0
    high = 0
    medium = 0
    
    # Count issues by severity
    if format_issues['date_of_birth_issues'] or format_issues['created_date_issues']:
        high += len(format_issues['date_of_birth_issues']) + len(format_issues['created_date_issues'])
    if invalid_values:
        high += len(invalid_values)
    if categorical_issues:
        medium += len(categorical_issues)
    if uniqueness['duplicates'] > 0:
        critical += uniqueness['duplicates']
    
    # Check for missing values
    for col, stats in completeness.items():
        if stats['missing'] > 0:
            if col in ['first_name', 'last_name', 'email', 'address']:
                medium += stats['missing']
            elif col in ['customer_id']:
                critical += stats['missing']
            else:
                medium += stats['missing']
    
    report.append(f"- Critical (blocks processing): {critical}")
    report.append(f"- High (data incorrect): {high}")
    report.append(f"- Medium (needs cleaning): {medium}")
    report.append("")
    
    report.append("=" * 50)
    report.append("Report generated: Part 1 Complete")
    
    return "\n".join(report)


def main():
    """Main function to run data quality analysis."""
    # Load data
    df = load_data('data/customer_raw.csv')
    
    # Run all analyses
    completeness = analyze_completeness(df)
    dtypes = analyze_data_types(df)
    format_issues = analyze_format_issues(df)
    uniqueness = analyze_uniqueness(df)
    invalid_values = analyze_invalid_values(df)
    categorical_issues = analyze_categorical_validity(df)
    
    # Generate report
    report = generate_report(df, completeness, dtypes, format_issues, 
                           uniqueness, invalid_values, categorical_issues)
    
    # Save report
    with open('docs/data_quality_report.txt', 'w', encoding='utf-8') as f:
        f.write(report)
    
    print("Data quality analysis complete!")
    print("Report saved to: docs/data_quality_report.txt")
    
    return report


if __name__ == "__main__":
    main()
