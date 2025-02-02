import os
from utils import validate_email, validate_date_time, validate_date
import uuid
import datetime as dt

class Application:
    
    def __init__(self, database, is_running=True):
        self.user_email = str()
        self.is_running = is_running
        self.db = database
        self.message = str()
        self.last_performances_select = []
    
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
            print('F - Show available seats')
            print('T - Show user tickets')
        else:
            print(f"{'L - Log in':<20}{'A - Create account':<20}")
        print(f"{'P - show programme':<20}{'S - Search':<20}")
        print(f"{'X - exit':<20}")

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
        dates = [ dt.datetime.today().date()+dt.timedelta(days=i) for i in range(14)]
        performances = self.db.select_performances_by_dates(dates)
        self.last_performances_select = []
        for i,performance in enumerate(performances):
            print(f"{i+1}. {performance.title:<32} {performance.start_date.strftime('%Y-%m-%d %H:%M')} - {performance.end_date.strftime('%H:%M')}")
            self.last_performances_select.append({'performance_id': performance.performance_id,
                                                'title': performance.title,
                                                'start_date': performance.start_date,
                                                'end_date':performance.end_date,
                                                'number_of_seats':performance.number_of_seats})
        input('Press any key to continue..')

    def add_performance(self):
        title = str()
        while len(title)<2:
            title = input("Performance's title: ")

        occurences = input("How many occurrences do you want to add?: ")
        occurences = int(occurences) if occurences.isdecimal() else 0
        
        start_dates = []
        end_dates = []
        numbers_of_seats = []
        
        for i in range(occurences):
            print(f'Occurence No. {i+1}')
            correct = False
            while not correct:
                correct = True

                start_date = input("Start date (yyyy‑mm‑dd HH:MM): ")
                if validate_date_time(start_date) is False:
                    correct = False

                end_date = input("End date (yyyy‑mm‑dd HH:MM): ")
                if validate_date_time(end_date) is False:
                    correct = False
                
                try:
                    number_of_seats = int(input("Number of seats: "))
                except:
                    correct = False

                if correct:
                    start_date =  dt.datetime.strptime(start_date, '%Y-%m-%d %H:%M')
                    end_date =  dt.datetime.strptime(end_date, '%Y-%m-%d %H:%M')
                    if start_date > end_date:
                        correct = False
                    else:
                        start_dates.append(start_date)
                        end_dates.append(end_date)
                        numbers_of_seats.append(number_of_seats)


        for i in range(occurences):
            performance_id = uuid.uuid1()
      
            p_date = start_dates[i].date()

            correct = self.db.insert_performance(p_date,
                                                start_dates[i], 
                                                title,       
                                                end_dates[i],
                                                performance_id,
                                                numbers_of_seats[i])
            if not correct:
                # check if performance is inserted ( maybe has been inserted but timeout occurs)
                performances = self.db.select_performances_by_dates([p_date], is_timeout_extended=True)
                success = False
                for perf in performances:
                    if perf.performance_id == performance_id:
                        success=True
                        self.message='Performance(s) has been added.'
                        return True
    
                self.message='Performance(s) has not been added.'
                return False

        self.message='Performance(s) has been added.'
        return True

    def search_performance(self):
        title = input('Title (or press enter if all): ')
        p_date = input('Date (yyyy‑mm‑dd) (or press enter if all): ')
        performances = []
        # only title        
        if len(title)>1 and (validate_date(p_date) is False):
            dates = [ dt.datetime.today().date()+dt.timedelta(days=i) for i in range(30)]
            performances = self.db.select_performances_by_dates(dates)
            performances = [p for p in performances if p.title == title.lower()]
        #  only date
        elif len(title)<=1 and validate_date(p_date):
            date = dt.datetime.strptime(p_date, '%Y-%m-%d')
            performances = self.db.select_performances_by_dates([date])
      
        # title and date
        elif len(title)>1 and validate_date(p_date):
            date = dt.datetime.strptime(p_date, '%Y-%m-%d')
            performances = self.db.select_performances_by_dates([date])
            performances = [p for p in performances if p.title == title.lower()]

        self.last_performances_select = []

        for i,performance in enumerate(performances):
            print(f"{i+1}. {performance.title:<32} {performance.start_date.strftime('%Y-%m-%d %H:%M')} - {performance.end_date.strftime('%H:%M')}")
            self.last_performances_select.append({'performance_id': performance.performance_id,
                                                'title': performance.title,
                                                'start_date': performance.start_date,
                                                'end_date':performance.end_date,
                                                'number_of_seats':performance.number_of_seats})
        


        input('Press any key to continue..')

    def buy_tickets(self, performance_id, title, start_date, number_of_seats):
        if not self.user_email:
            self.message = 'User needs to be logged in'
            return
        try:
            num_tickets = int(input('How many tickets would you like to buy: '))
        except:
            self.message = 'Wrong input'
            return
        
        seat_numbers = [int(input(f'Enter seat number for ticket {i+1}: ')) for i in range(num_tickets)]
        for seat in seat_numbers:
            if seat > number_of_seats or seat < 1:
                self.message = f"Seat number must be in range from 1 to {number_of_seats}."
                return
        first_names = []
        last_names = []
        for seat_number in seat_numbers:
            first_names.append(input(f'Enter first name of the ticket (seat {seat_number}) owner: '))
            last_names.append(input(f'Enter last name of the ticket (seat {seat_number}) owner: '))

        buy_timestamp = dt.datetime.today()
        success = self.db.insert_performance_seats_batch(performance_id, seat_numbers, title, start_date, self.user_email)
        if not success:
            seats = self.db.select_performance_seats(performance_id, is_timeout_extended=True)
            seats_taken = 0
            for seat in seats:
                if seat.seat_number in seat_numbers and seat.taken_by == self.user_email:
                    seats_taken+=1
            if seats_taken==len(seat_numbers):
                success = True
        
        if success:
            result = self.db.insert_user_ticket_batch(self.user_email, buy_timestamp, performance_id, seat_numbers, first_names, last_names)
            if result:
                self.message = 'Tickets have been bought'
            else:
                self.message = 'Could not buy tickets, contact support'
 
        else:
            self.message = 'Could not buy tickets, make sure seats are aviable'
 

    def show_user_tickets(self):
        if not self.user_email:
            self.message = 'User needs to be logged in'
            input('Press any key to continue..')
            return

        start_date = input('Start date (yyyy‑mm‑dd) (or press enter if all): ')
        end_date = input('End date (yyyy‑mm‑dd) (or press enter if all): ')

        if validate_date(start_date) is False:
            start_date = dt.datetime.min
        else:
            start_date = dt.datetime.strptime(start_date, '%Y-%m-%d')

        if validate_date(end_date) is False:
            end_date = dt.datetime.max
        else:
            end_date = dt.datetime.strptime(end_date, '%Y-%m-%d')

        tickets = self.db.select_user_tickets(self.user_email, start_date, end_date)

        for i, ticket in enumerate(tickets):
            ticket_info = self.db.select_performance_seat_performance_info(ticket.performance_id, ticket.seat_number)
            print(f'{i+1}.')
            if ticket_info and ticket_info.title:
                print(f'Title: {ticket_info.title}')
                print(f"Date: {ticket_info.start_date.strftime('%Y-%m-%d %H:%M')}")
            print(f"Bought: {ticket.buy_timestamp.strftime('%Y-%m-%d %H:%M')}")
            print(f'Seat number: {ticket.seat_number}')
            print(f'First name: {ticket.first_name}')
            print(f'Last name: {ticket.last_name}\n')

        input('Press any key to continue..')
    
    def show_performance_seats(self):
        if(len(self.last_performances_select) == 0):
            self.message = 'Search performances first'
            return
        for i,performance in enumerate(self.last_performances_select):
            print(f"{i+1}. {performance['title']:<32} {performance['start_date'].strftime('%Y-%m-%d %H:%M')} - {performance['end_date'].strftime('%H:%M')}")

        performance_number = int(input('Enter chosen performance number: ')) - 1

        if performance_number >= len(self.last_performances_select):
            self.message = 'performance number is too big'
            return
        
        performance_id = self.last_performances_select[performance_number]['performance_id']

        seats_taken = self.db.select_performance_seats(performance_id)

        seats = [False for i in range(self.last_performances_select[performance_number]['number_of_seats'])]
        str_to_print = ''
        for seat in seats_taken:
            seats[seat.seat_number] = True
        for i, taken in enumerate(seats):
            if taken:
                str_to_print =  str_to_print + 'XX '
            else:
                str_to_print = str_to_print + f'{str(i+1):<2}' + ' '
            if (i+1)%10 == 0:
                str_to_print = str_to_print + '\n'
        print(str_to_print)
        wanna_buy_ticket = input('Would you like to buy a ticket? [Y - yes / N - no]: ')

        if wanna_buy_ticket.upper() == 'Y':
            self.buy_tickets(performance_id,
                            self.last_performances_select[performance_number]['title'],
                            self.last_performances_select[performance_number]['start_date'],
                            self.last_performances_select[performance_number]['number_of_seats'])

    def do_action(self, action):
        switcher={
                'X':self.stop_app,
                'A':self.create_account,
                'L':self.log_in,
                'O':self.log_out,
                'P':self.show_programme,
                'N':self.add_performance,
                'S':self.search_performance,
                'F':self.show_performance_seats,
                'T':self.show_user_tickets
                }
        func=switcher.get(action.upper(),lambda : None)
        func()

