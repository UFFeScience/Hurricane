import calendar

from datetime import date
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pytz import timezone

class DateTime:

    def get_date_tomorrow(self):
        return date.today() + timedelta(days=1)

    def get_month_year(self):
        return datetime.strftime(datetime.now(), '%Y_%m')
    
    def get_first_day_last_month(self):
        first_day = (datetime.now().replace(day=1) - timedelta(days=1)).replace(day=1).date()
        return first_day
    
    def get_first_day_current_month(self):
        first_day = datetime.now().replace(day=1).date()
        return first_day
