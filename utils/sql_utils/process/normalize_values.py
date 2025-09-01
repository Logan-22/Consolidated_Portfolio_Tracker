from datetime import datetime, date

def normalize_payload_and_existing_values(value, column_type):
    if value is None:
        return None

    column_data_type = column_type['type']
    if column_data_type == 'NUMBER':
        scale = column_type.get('scale', 4) or 0
        try:
            return round(float(value), scale)
        except Exception:
            print(f'Bad Numeric Data sent: {value}')
            return value
    elif column_data_type == 'DATE':
        if isinstance(value, datetime):
            return value
        elif isinstance(value, str):
            try:
                return datetime.strptime(value.strip(), '%Y-%m-%d').date()
            except Exception:
                    print(f'Bad Date Data sent: {value}')
                    return value
        elif isinstance(value, date):
            return value
    elif column_data_type == 'STRING':
        return value.strip().lower()
    else:
        print(f'Bad Value sent: {value}')
        return value
