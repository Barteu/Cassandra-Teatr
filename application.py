import os
from utils import validate_email, validate_date
import uuid
import datetime as dt

class Application:
    
    def __init__(self, database, is_running=True):
        self.user_email = str()
        self.is_running = is_running
        self.db = database
        self.message = str()
    
    def cls(self):
        os.system('cls' if os.name=='nt' else 'clear')

    def show_menu(self):
        self.cls()
        print(f"-----------[{'USER: '+(self.user_email if self.user_email else 'not logged in'):<24}]-----------")
        self.show_actions()
        print('------------------------------------------------')
        print(self.message)
        self.message = str()

    def show_actions(self):
        if self.user_email:
            print('O - Log out')
        else:
            print(f"{'L - Log in':<20}{'A - Create account':<20}")
        print(f"{'X - exit':<20}{'P - show programme':<20}")

        # if user is a staff member
        if self.user_email and self.user_email.split('@')[1] == 'theatre.com':
            print("N - Add performance")

    def stop_app(self):
        self.is_running = False

    def create_account(self):

        email = str()
        first_name = str()
        last_name = str()
        while not validate_email(email):
            email = input('Email: ')
        while len(first_name)<3:
            first_name = input('First name: ')
        while len(last_name)<3:
            last_name = input('Last Name: ')

        if self.db.insert_user(email, first_name, last_name):
            self.user_email = email
            self.message = 'Account has been created'
        else:
            self.message = 'Email is already used'

    def log_in(self):
        email = str()
        while not validate_email(email):
            email = input('Email: ')
        user = self.db.select_user(email)
        if user:
            self.user_email = email
            self.message ='You logged in'
            return True
        else:
            self.message ='Incorrect email'
            return False

    def log_out(self):
        self.message ='You have been logged out'
        self.user_email = str()

    def show_programme(self):
        performances = self.db.select_current_performances()
        for i,performance in enumerate(performances):
            print(f"{i+1}. {performance.title:<32} {performance.start_date.strftime('%Y-%m-%d %H:%M')} - {performance.end_date.strftime('%H:%M')}")
        input('Press any key to continue..')

    def add_performance(self):
        title = str()
        while len(title)<2:
            title = input("Performance's title: ")

        occurences = input("How many occurrences do you want to add?: ")
        occurences = int(occurences) if occurences.isdecimal() else 0
        
        start_dates = []
        end_dates = []
        
        for i in range(occurences):
            print(f'Occurence No. {i+1}')
            correct = False
            while not correct:
                correct = True

                start_date = input("Start date (yyyy‑mm‑dd HH:MM): ")
                if validate_date(start_date) is False:
                    correct = False

                end_date = input("End date (yyyy‑mm‑dd HH:MM): ")
                if validate_date(end_date) is False:
                    correct = False
                
                if correct:
                    start_date =  dt.datetime.strptime(start_date, '%Y-%m-%d %H:%M')
                    end_date =  dt.datetime.strptime(end_date, '%Y-%m-%d %H:%M')
                    if start_date > end_date:
                        correct = False
                    else:
                        start_dates.append(start_date)
                        end_dates.append(end_date)


        for i in range(occurences):
            performance_id = uuid.uuid1()
            self.db.insert_performance(title, 
                                       start_dates[i],
                                       end_dates[i],
                                       performance_id)

        
    def do_action(self, action):
        switcher={
                'X':self.stop_app,
                'A':self.create_account,
                'L':self.log_in,
                'O':self.log_out,
                'P':self.show_programme,
                'N':self.add_performance
                }
        func=switcher.get(action.upper(),lambda : None)
        func()

