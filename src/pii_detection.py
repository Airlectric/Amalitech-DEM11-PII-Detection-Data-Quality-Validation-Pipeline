"""
PII Detection Module
Part 2: Detect Personally Identifiable Information
"""

import pandas as pd
import re
from collections import defaultdict


def load_data(filepath):
    """Load CSV file into pandas DataFrame."""
    df = pd.read_csv(filepath)
    df.columns = df.columns.str.strip()
    return df


def detect_emails(df):
    """Detect and count email addresses using regex."""
    email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
    
    emails_found = []
    for idx, email in enumerate(df['email']):
        if pd.notna(email) and str(email).strip():
            if email_pattern.match(str(email).strip()):
                emails_found.append({
                    'row': idx + 2,
                    'value': str(email).strip()
                })
    
    return emails_found


def detect_phone_numbers(df):
    """Detect and count phone numbers using regex."""
    # Pattern to match various phone formats
    phone_patterns = [
        re.compile(r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'),
        re.compile(r'^\d{10}$'),
        re.compile(r'^\d{3}[-.]\d{3}[-.]\d{4}$')
    ]
    
    phones_found = []
    for idx, phone in enumerate(df['phone']):
        if pd.notna(phone) and str(phone).strip():
            phone_str = str(phone).strip()
            for pattern in phone_patterns:
                if pattern.match(phone_str):
                    phones_found.append({
                        'row': idx + 2,
                        'value': phone_str
                    })
                    break
    
    return phones_found


def detect_names(df):
    """Detect and count names (first and last)."""
    names_found = []
    
    for idx in range(len(df)):
        first_name = df.loc[idx, 'first_name']
        last_name = df.loc[idx, 'last_name']
        
        has_pii = False
        name_data = {'row': idx + 2, 'first_name': None, 'last_name': None}
        
        if pd.notna(first_name) and str(first_name).strip():
            name_data['first_name'] = str(first_name).strip()
            has_pii = True
        
        if pd.notna(last_name) and str(last_name).strip():
            name_data['last_name'] = str(last_name).strip()
            has_pii = True
        
        if has_pii:
            names_found.append(name_data)
    
    return names_found


def detect_addresses(df):
    """Detect and count addresses."""
    addresses_found = []
    
    for idx, address in enumerate(df['address']):
        if pd.notna(address) and str(address).strip():
            addresses_found.append({
                'row': idx + 2,
                'value': str(address).strip()
            })
    
    return addresses_found


def detect_dates_of_birth(df):
    """Detect and count dates of birth."""
    dobs_found = []
    
    for idx, dob in enumerate(df['date_of_birth']):
        if pd.notna(dob) and str(dob).strip():
            dob_str = str(dob).strip()
            if dob_str != 'invalid_date':
                dobs_found.append({
                    'row': idx + 2,
                    'value': dob_str
                })
    
    return dobs_found


def analyze_pii_risk(emails, phones, names, addresses, dobs, total_rows):
    """Analyze the risk of PII exposure."""
    risk_assessment = {
        'high_risk': {
            'fields': ['Names', 'Emails', 'Phone Numbers', 'Addresses', 'Dates of Birth'],
            'exposure': 'Identity theft, phishing, social engineering attacks'
        },
        'medium_risk': {
            'fields': ['Income', 'Account Status'],
            'exposure': 'Financial profiling, targeted marketing'
        }
    }
    
    exposure_stats = {
        'emails': {
            'count': len(emails),
            'percentage': round((len(emails) / total_rows) * 100, 1),
            'risk': 'HIGH - Can be used for phishing and spam'
        },
        'phones': {
            'count': len(phones),
            'percentage': round((len(phones) / total_rows) * 100, 1),
            'risk': 'HIGH - Can be used for social engineering and fraud'
        },
        'names': {
            'count': len(names),
            'percentage': round((len(names) / total_rows) * 100, 1),
            'risk': 'HIGH - Enables identity spoofing'
        },
        'addresses': {
            'count': len(addresses),
            'percentage': round((len(addresses) / total_rows) * 100, 1),
            'risk': 'HIGH - Physical location exposure'
        },
        'dobs': {
            'count': len(dobs),
            'percentage': round((len(dobs) / total_rows) * 100, 1),
            'risk': 'HIGH - Used for identity verification bypass'
        }
    }
    
    return risk_assessment, exposure_stats


def generate_pii_report(df, emails, phones, names, addresses, dobs, risk_assessment, exposure_stats):
    """Generate the PII detection report."""
    total_rows = len(df)
    
    report = []
    report.append("PII DETECTION REPORT")
    report.append("=" * 50)
    report.append("")
    
    # Risk Assessment
    report.append("RISK ASSESSMENT:")
    report.append("- HIGH: Names, Emails, Phone Numbers, Addresses, Dates of Birth")
    report.append("- MEDIUM: Income (financial sensitivity)")
    report.append("")
    
    # Detected PII Summary
    report.append("DETECTED PII:")
    report.append(f"- Emails found: {exposure_stats['emails']['count']} ({exposure_stats['emails']['percentage']}%)")
    report.append(f"- Phone numbers found: {exposure_stats['phones']['count']} ({exposure_stats['phones']['percentage']}%)")
    report.append(f"- Names found: {exposure_stats['names']['count']} ({exposure_stats['names']['percentage']}%)")
    report.append(f"- Addresses found: {exposure_stats['addresses']['count']} ({exposure_stats['addresses']['percentage']}%)")
    report.append(f"- Dates of birth found: {exposure_stats['dobs']['count']} ({exposure_stats['dobs']['percentage']}%)")
    report.append("")
    
    # Detailed findings
    report.append("DETAILED FINDINGS:")
    report.append("")
    
    report.append("1. EMAIL ADDRESSES:")
    for email in emails:
        report.append(f"   - Row {email['row']}: {email['value']}")
    report.append("")
    
    report.append("2. PHONE NUMBERS:")
    for phone in phones:
        report.append(f"   - Row {phone['row']}: {phone['value']}")
    report.append("")
    
    report.append("3. NAMES:")
    for name in names:
        full_name = " ".join(filter(None, [name['first_name'], name['last_name']]))
        report.append(f"   - Row {name['row']}: {full_name}")
    report.append("")
    
    report.append("4. ADDRESSES:")
    for addr in addresses:
        report.append(f"   - Row {addr['row']}: {addr['value']}")
    report.append("")
    
    report.append("5. DATES OF BIRTH:")
    for dob in dobs:
        report.append(f"   - Row {dob['row']}: {dob['value']}")
    report.append("")
    
    # Exposure Risk Analysis
    report.append("EXPOSURE RISK:")
    report.append("If this dataset were breached, attackers could:")
    report.append("- Phish customers (have emails)")
    report.append("- Spoof identities (have names + DOB + address)")
    report.append("- Social engineer (have phone numbers)")
    report.append("- Conduct targeted attacks using financial data")
    report.append("")
    
    report.append("MITIGATION: Mask all PII before sharing with analytics teams")
    report.append("")
    
    report.append("=" * 50)
    
    return "\n".join(report)


def main():
    """Main function to run PII detection."""
    # Load data
    df = load_data('data/customer_raw.csv')
    total_rows = len(df)
    
    print(f"Scanning {total_rows} rows for PII...")
    
    # Detect PII in each category
    emails = detect_emails(df)
    phones = detect_phone_numbers(df)
    names = detect_names(df)
    addresses = detect_addresses(df)
    dobs = detect_dates_of_birth(df)
    
    # Analyze risk
    risk_assessment, exposure_stats = analyze_pii_risk(
        emails, phones, names, addresses, dobs, total_rows
    )
    
    # Generate report
    report = generate_pii_report(
        df, emails, phones, names, addresses, dobs, 
        risk_assessment, exposure_stats
    )
    
    # Save report
    with open('docs/pii_detection_report.txt', 'w', encoding='utf-8') as f:
        f.write(report)
    
    print("PII detection complete!")
    print("Report saved to: docs/pii_detection_report.txt")
    print(f"\nSummary:")
    print(f"- Emails: {len(emails)}")
    print(f"- Phones: {len(phones)}")
    print(f"- Names: {len(names)}")
    print(f"- Addresses: {len(addresses)}")
    print(f"- DOBs: {len(dobs)}")
    
    return report


if __name__ == "__main__":
    main()
