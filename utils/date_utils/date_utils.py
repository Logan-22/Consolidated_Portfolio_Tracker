def convert_weekday_from_int_to_char(weekday_int):
    weekday_switch = {
        0 : 'Monday',
        1 : 'Tuesday',
        2 : 'Wednesday',
        3 : 'Thursday',
        4 : 'Friday',
        5 : 'Saturday',
        6 : 'Sunday'
    }
    return weekday_switch.get(weekday_int, "N/A")