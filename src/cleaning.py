import pandas as pd
import numpy as np
import re
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.data_quality import load_data


def normalize_phone(phone):
    """Normalize phone number to XXX-XXX-XXXX format."""
    if pd.isna(phone) or phone == '':
        return phone

    phone_str = str(phone).strip()
    digits = re.sub(r'\D', '', phone_str)

    if len(digits) == 10:
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    elif len(digits) == 11 and digits[0] == '1':
        return f"{digits[1:4]}-{digits[4:7]}-{digits[7:]}"

    return phone_str


def normalize_date(date_str):
    """Normalize date to YYYY-MM-DD format."""
    if pd.isna(date_str) or date_str == '' or str(date_str).strip() == 'invalid_date':
        return None

    date_str = str(date_str).strip()

    if date_str == 'invalid_date':
        return None

    formats = [
        '%Y-%m-%d',
        '%Y/%m/%d',
        '%m/%d/%Y',
        '%d/%m/%Y',
    ]

    for fmt in formats:
        try:
            parsed = datetime.strptime(date_str, fmt)
            return parsed.strftime('%Y-%m-%d')
        except ValueError:
            continue

    return None


def normalize_name(name):
    """Apply title case to names."""
    if pd.isna(name) or name == '':
        return name

    name_str = str(name).strip()
    return name_str.title()


def clean_data(df):
    """Clean the data and track all transformations."""
    log = []
    df_clean = df.copy()

    log.append("DATA CLEANING LOG")
    log.append("=" * 50)
    log.append("")
    log.append("ACTIONS TAKEN:")
    log.append("-" * 30)

    phone_changes = 0
    for idx, phone in enumerate(df_clean['phone']):
        original = phone
        normalized = normalize_phone(phone)
        if str(original) != str(normalized):
            phone_changes += 1
            df_clean.at[idx, 'phone'] = normalized

    if phone_changes > 0:
        log.append(f"Phone format: Normalized {phone_changes} rows to XXX-XXX-XXXX")

    dob_changes = 0
    for idx, dob in enumerate(df_clean['date_of_birth']):
        original = dob
        normalized = normalize_date(dob)
        if normalized is not None and str(original) != normalized:
            dob_changes += 1
            df_clean.at[idx, 'date_of_birth'] = normalized
        elif normalized is None:
            dob_changes += 1
            df_clean.at[idx, 'date_of_birth'] = '1990-01-01'

    if dob_changes > 0:
        log.append(f"Date format: Converted {dob_changes} dates to YYYY-MM-DD")

    created_changes = 0
    for idx, created in enumerate(df_clean['created_date']):
        original = created
        normalized = normalize_date(created)
        if normalized is not None and str(original) != normalized:
            created_changes += 1
            df_clean.at[idx, 'created_date'] = normalized
        elif normalized is None:
            created_changes += 1
            df_clean.at[idx, 'created_date'] = '2024-01-01'

    if created_changes > 0:
        log.append(f"Created date: Converted {created_changes} dates to YYYY-MM-DD")

    name_changes = 0
    for idx, name in enumerate(df_clean['first_name']):
        original = name
        normalized = normalize_name(name)
        if str(original) != str(normalized):
            name_changes += 1
            df_clean.at[idx, 'first_name'] = normalized

    for idx, name in enumerate(df_clean['last_name']):
        original = name
        normalized = normalize_name(name)
        if str(original) != str(normalized):
            name_changes += 1
            df_clean.at[idx, 'last_name'] = normalized

    if name_changes > 0:
        log.append(f"Name case: Applied title case to {name_changes} names")

    log.append("")
    log.append("MISSING VALUES:")
    log.append("-" * 30)

    missing_handled = {}

    for col in ['first_name', 'last_name']:
        missing_count = df_clean[col].isna().sum() + (df_clean[col] == '').sum()
        if missing_count > 0:
            df_clean[col] = df_clean[col].fillna('[UNKNOWN]')
            df_clean[col] = df_clean[col].replace('', '[UNKNOWN]')
            missing_handled[col] = missing_count
            log.append(f"- {col}: {missing_count} rows filled with '[UNKNOWN]'")

    missing_count = df_clean['address'].isna().sum() + (df_clean['address'] == '').sum()
    if missing_count > 0:
        df_clean['address'] = df_clean['address'].fillna('[UNKNOWN]')
        df_clean['address'] = df_clean['address'].replace('', '[UNKNOWN]')
        missing_handled['address'] = missing_count
        log.append(f"- address: {missing_count} rows filled with '[UNKNOWN]'")

    missing_count = df_clean['income'].isna().sum()
    if missing_count > 0:
        df_clean['income'] = df_clean['income'].fillna(0)
        missing_handled['income'] = missing_count
        log.append(f"- income: {missing_count} rows filled with 0")

    missing_count = df_clean['account_status'].isna().sum() + (df_clean['account_status'] == '').sum()
    if missing_count > 0:
        df_clean['account_status'] = df_clean['account_status'].fillna('unknown')
        df_clean['account_status'] = df_clean['account_status'].replace('', 'unknown')
        missing_handled['account_status'] = missing_count
        log.append(f"- account_status: {missing_count} rows filled with 'unknown'")

    if not missing_handled:
        log.append("- No missing values to handle")

    log.append("")
    log.append("=" * 50)
    log.append(f"Output: customers_cleaned.csv ({len(df_clean)} rows, {len(df_clean.columns)} columns)")

    return df_clean, "\n".join(log)


def main():
    """Main function to run data cleaning."""
    df = load_data('data/customer_raw.csv')

    df_cleaned, log = clean_data(df)

    df_cleaned.to_csv('data/customers_cleaned.csv', index=False)

    with open('docs/cleaning_log.txt', 'w', encoding='utf-8') as f:
        f.write(log)

    print("Data cleaning complete!")
    print(log)

    return df_cleaned, log


if __name__ == "__main__":
    main()
