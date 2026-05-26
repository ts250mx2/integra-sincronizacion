import decimal
import logging
from datetime import datetime

def valida_nulo(value, is_string=False):
    """
    Simulates the VB6 ValidaNulos function.
    Returns:
        If value is None:
            "" if is_string is True else 0 (or "0")
        If value is not None:
            The escaped string if is_string is True
            A numeric or string representation otherwise
    """
    if value is None:
        return "" if is_string else 0

    if isinstance(value, bool):
        return 1 if value else 0

    if isinstance(value, (int, float, decimal.Decimal)):
        # If is_string was requested for a number, return it as string
        if is_string:
            return str(value).replace("'", "''")
        return value

    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')

    # Default string handling
    val_str = str(value)
    if is_string:
        return val_str.replace("'", "''")
    return val_str

def format_sql_date(date_val):
    """
    Formats a datetime object to a SQL compliant string.
    If value is None, returns a safe default '2000-01-01 00:00:00'.
    """
    if not date_val:
        return "2000-01-01 00:00:00"
    if isinstance(date_val, str):
        # Clean potential single quotes
        return date_val.strip("'")
    if isinstance(date_val, datetime):
        return date_val.strftime('%Y-%m-%d %H:%M:%S')
    return str(date_val)

def campo_requerido(cursor, sql, field_name):
    """
    Fetches a single field value from the query result.
    Case-insensitive matching of column/field name.
    """
    try:
        cursor.execute(sql)
    except Exception as e:
        logging.error(f"Error executing query in campo_requerido: {e}")
        logging.error(f"SQL: {sql}")
        raise

    row = cursor.fetchone()
    if row:
        # Case 1: Dictionary row (e.g. mysql cursor with dictionary=True)
        if isinstance(row, dict):
            for k, v in row.items():
                if k.lower() == field_name.lower():
                    return v

        # Case 2: Object/Attribute row (e.g. pyodbc.Row)
        try:
            return getattr(row, field_name)
        except AttributeError:
            pass

        # Case 3: List/Tuple row with description matching
        if cursor.description:
            description = [d[0].lower() for d in cursor.description]
            if field_name.lower() in description:
                idx = description.index(field_name.lower())
                return row[idx]
                
    return None
