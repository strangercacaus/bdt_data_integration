from datetime import datetime,timedelta

class Utils():
    def get_current_formatted_date():
        utc_offset = timedelta(hours=-3)
        return (datetime.now() + utc_offset).date()