import re
import datetime as dt

def validate_email(email):
    regex = re.compile(r'([A-Za-z0-9]+[.-_])*[A-Za-z0-9]+@[A-Za-z0-9-]+(\.[A-Z|a-z]{2,})+')
    if re.fullmatch(regex, email):
      return True
    else:
      return False

def validate_date_time(date_text):
      try:
          dt.datetime.strptime(date_text, '%Y-%m-%d %H:%M')
          return True
      except ValueError:
          return False

def validate_date(date_text):
      try:
          dt.datetime.strptime(date_text, '%Y-%m-%d')
          return True
      except ValueError:
          return False

