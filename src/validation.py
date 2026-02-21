"""
Data Validation Module
Part 3: Build a Data Validator
"""

import pandas as pd
import re
from datetime import datetime
from collections import defaultdict


def load_data(filepath):
    """Load CSV file into pandas DataFrame."""
    df = pd.read_csv(filepath)
    df.columns = df.columns.str.strip()
    return df


class DataValidator:
    """Validator class to check data against schema rules."""
    
    def __init__(self, df):
        self.df = df
        self.failures = []
        self.passed_rows = set(range(len(df)))
    
    def validate_customer_id(self):
        """Validate customer_id: unique, positive integer."""
        ids_seen = set()
        
        for idx, value in enumerate(self.df['customer_id']):
            issues = []
            
            # Check if positive integer
            if pd.isna(value):
                issues.append("Empty (should be non-empty)")
            else:
                try:
                    val = int(value)
                    if val <= 0:
                        issues.append(f"{value} (should be positive)")
                except (ValueError, TypeError):
                    issues.append(f"{value} (should be integer)")
            
            # Check uniqueness
            if pd.notna(value):
                if value in ids_seen:
                    issues.append(f"{value} (duplicate ID)")
                else:
                    ids_seen.add(value)
            
            if issues:
                self.add_failure(idx, 'customer_id', issues)
    
    def validate_name(self, column):
        """Validate name fields: non-null, letters only, 2-50 chars."""
        for idx, value in enumerate(self.df[column]):
            issues = []
            
            if pd.isna(value) or str(value).strip() == '':
                issues.append("Empty (should be non-empty)")
            else:
                name = str(value).strip()
                
                # Check length
                if len(name) < 2:
                    issues.append(f"'{name}' (too short, min 2 chars)")
                elif len(name) > 50:
                    issues.append(f"'{name}' (too long, max 50 chars)")
                
                # Check alphabetic (allow spaces and hyphens for compound names)
                if not re.match(r'^[a-zA-Z\s\-]+$', name):
                    issues.append(f"'{name}' (should be alphabetic)")
            
            if issues:
                self.add_failure(idx, column, issues)
    
    def validate_email(self):
        """Validate email: valid email format."""
        email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        
        for idx, value in enumerate(self.df['email']):
            issues = []
            
            if pd.isna(value) or str(value).strip() == '':
                issues.append("Empty (should be non-empty)")
            else:
                email = str(value).strip()
                if not email_pattern.match(email):
                    issues.append(f"'{email}' (invalid email format)")
            
            if issues:
                self.add_failure(idx, 'email', issues)
    
    def validate_phone(self):
        """Validate phone: reasonable length and format."""
        for idx, value in enumerate(self.df['phone']):
            issues = []
            
            if pd.isna(value) or str(value).strip() == '':
                issues.append("Empty (should be non-empty)")
            else:
                phone = str(value).strip()
                # Extract digits only
                digits = re.sub(r'\D', '', phone)
                
                if len(digits) != 10:
                    issues.append(f"'{phone}' (should have 10 digits, found {len(digits)})")
            
            if issues:
                self.add_failure(idx, 'phone', issues)
    
    def validate_date(self, column):
        """Validate date fields: valid date, YYYY-MM-DD format."""
        for idx, value in enumerate(self.df[column]):
            issues = []
            
            if pd.isna(value) or str(value).strip() == '':
                issues.append("Empty (should be non-empty)")
            else:
                date_str = str(value).strip()
                
                # Check for invalid_date string
                if date_str.lower() == 'invalid_date':
                    issues.append("'invalid_date' (invalid date value)")
                else:
                    # Try to parse date
                    parsed = False
                    formats = ['%Y-%m-%d', '%Y/%m/%d', '%m/%d/%Y']
                    
                    for fmt in formats:
                        try:
                            datetime.strptime(date_str, fmt)
                            parsed = True
                            break
                        except ValueError:
                            continue
                    
                    if not parsed:
                        issues.append(f"'{date_str}' (unrecognized date format)")
            
            if issues:
                self.add_failure(idx, column, issues)
    
    def validate_address(self):
        """Validate address: non-empty, 10-200 chars."""
        for idx, value in enumerate(self.df['address']):
            issues = []
            
            if pd.isna(value) or str(value).strip() == '':
                issues.append("Empty (should be non-empty)")
            else:
                address = str(value).strip()
                
                if len(address) < 10:
                    issues.append(f"'{address}' (too short, min 10 chars)")
                elif len(address) > 200:
                    issues.append(f"'{address}' (too long, max 200 chars)")
            
            if issues:
                self.add_failure(idx, 'address', issues)
    
    def validate_income(self):
        """Validate income: non-negative, ≤ $10M."""
        for idx, value in enumerate(self.df['income']):
            issues = []
            
            if pd.isna(value):
                issues.append("Empty (should be non-empty)")
            else:
                try:
                    income_val = float(value)
                    
                    if income_val < 0:
                        issues.append(f"{income_val} (should be non-negative)")
                    elif income_val > 10000000:
                        issues.append(f"{income_val} (exceeds $10M limit)")
                except (ValueError, TypeError):
                    issues.append(f"'{value}' (should be numeric)")
            
            if issues:
                self.add_failure(idx, 'income', issues)
    
    def validate_account_status(self):
        """Validate account_status: must be active, inactive, or suspended."""
        valid_statuses = {'active', 'inactive', 'suspended'}
        
        for idx, value in enumerate(self.df['account_status']):
            issues = []
            
            if pd.isna(value) or str(value).strip() == '':
                issues.append("Empty (should be one of: active, inactive, suspended)")
            else:
                status = str(value).strip().lower()
                if status not in valid_statuses:
                    issues.append(f"'{value}' (invalid value, should be: active, inactive, suspended)")
            
            if issues:
                self.add_failure(idx, 'account_status', issues)
    
    def add_failure(self, row_idx, column, issues):
        """Add a validation failure."""
        self.failures.append({
            'row': row_idx + 2,  # +2 for 1-indexed and header
            'column': column,
            'issues': issues
        })
        if row_idx in self.passed_rows:
            self.passed_rows.remove(row_idx)
    
    def run_all_validations(self):
        """Run all validation rules."""
        print("Running validations...")
        
        self.validate_customer_id()
        print("OK Validated customer_id")
        
        self.validate_name('first_name')
        print("OK Validated first_name")
        
        self.validate_name('last_name')
        print("OK Validated last_name")
        
        self.validate_email()
        print("OK Validated email")
        
        self.validate_phone()
        print("OK Validated phone")
        
        self.validate_date('date_of_birth')
        print("OK Validated date_of_birth")
        
        self.validate_address()
        print("OK Validated address")
        
        self.validate_income()
        print("OK Validated income")
        
        self.validate_account_status()
        print("OK Validated account_status")
        
        self.validate_date('created_date')
        print("OK Validated created_date")
        
        print(f"\nValidation complete: {len(self.passed_rows)}/{len(self.df)} rows passed")
    
    def get_failures_by_column(self):
        """Group failures by column."""
        by_column = defaultdict(list)
        for failure in self.failures:
            by_column[failure['column']].append(failure)
        return by_column


def generate_validation_report(validator, df):
    """Generate the validation results report."""
    total_rows = len(df)
    passed_count = len(validator.passed_rows)
    failed_count = total_rows - passed_count
    failures_by_column = validator.get_failures_by_column()
    
    report = []
    report.append("VALIDATION RESULTS")
    report.append("=" * 50)
    report.append("")
    
    # Summary
    report.append(f"PASS: {passed_count} rows passed all checks")
    report.append(f"FAIL: {failed_count} rows failed")
    report.append("")
    
    # Failures by column
    report.append("FAILURES BY COLUMN:")
    report.append("-" * 30)
    report.append("")
    
    column_order = [
        'customer_id', 'first_name', 'last_name', 'email', 
        'phone', 'date_of_birth', 'address', 'income', 
        'account_status', 'created_date'
    ]
    
    for column in column_order:
        if column in failures_by_column:
            report.append(f"{column}:")
            for failure in failures_by_column[column]:
                for issue in failure['issues']:
                    report.append(f"  - Row {failure['row']}: {issue}")
            report.append("")
    
    # Summary statistics
    report.append("VALIDATION SUMMARY:")
    report.append("-" * 30)
    report.append(f"- customer_id: {total_rows - len(failures_by_column.get('customer_id', []))}/{total_rows} valid")
    report.append(f"- first_name: {total_rows - len(failures_by_column.get('first_name', []))}/{total_rows} valid")
    report.append(f"- last_name: {total_rows - len(failures_by_column.get('last_name', []))}/{total_rows} valid")
    report.append(f"- email: {total_rows - len(failures_by_column.get('email', []))}/{total_rows} valid")
    report.append(f"- phone: {total_rows - len(failures_by_column.get('phone', []))}/{total_rows} valid")
    report.append(f"- date_of_birth: {total_rows - len(failures_by_column.get('date_of_birth', []))}/{total_rows} valid")
    report.append(f"- address: {total_rows - len(failures_by_column.get('address', []))}/{total_rows} valid")
    report.append(f"- income: {total_rows - len(failures_by_column.get('income', []))}/{total_rows} valid")
    report.append(f"- account_status: {total_rows - len(failures_by_column.get('account_status', []))}/{total_rows} valid")
    report.append(f"- created_date: {total_rows - len(failures_by_column.get('created_date', []))}/{total_rows} valid")
    report.append("")
    
    report.append("=" * 50)
    report.append("Report generated: Part 3 Complete")
    
    return "\n".join(report)


def main():
    """Main function to run data validation."""
    # Load data
    df = load_data('data/customer_raw.csv')
    print(f"Validating {len(df)} rows against schema rules...\n")
    
    # Create validator and run validations
    validator = DataValidator(df)
    validator.run_all_validations()
    
    # Generate report
    report = generate_validation_report(validator, df)
    
    # Save report
    with open('docs/validation_results.txt', 'w', encoding='utf-8') as f:
        f.write(report)
    
    print("\nValidation report saved to: notes/validation_results.txt")
    
    return report


if __name__ == "__main__":
    main()
