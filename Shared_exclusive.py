from contextlib import contextmanager
from threading  import Lock, Thread
import random
from time import sleep
import operator as _

# Implement the shared excusive lock algorithm
class SharedExclusive(object):

    def __init__(self):
        self.write_lock_obj = Lock()
        self.num_r = 0 # Number of read_locks

    def r_acquire(self):
        print("\nRead_lock Called:")
        if self.write_lock_obj.locked():
            print("Wait for write_lock")
            
            # waiting until write_lock released...
            while self.write_lock_obj.locked():
                print(".",end="")
            self.num_r += 1
            print(f"read item's qeue num:{self.num_r}")
        else:
            sleep(2)
            self.num_r += 1
            print(f"read item's qeue num:{self.num_r}")

    def r_release(self):
        if self.num_r > 0:
            self.num_r -= 1
            print(f"read item's qeue num after release:{self.num_r}")

    @contextmanager
    def r_locked(self):
        try:
            self.r_acquire()
            yield
        finally:
            self.r_release()

    def w_acquire(self):
        print("\nWrite_lock Called:")
        if self.num_r:
            print("Wait for read_lock")
            
            # waiting until read_lock released...
            while self.num_r:
                print("*",end="")
            print('\nWrite lock after read lock released:\n')
            self.write_lock_obj.acquire()
            
        elif self.write_lock_obj.locked():
            print("Wait for write_lock")
            self.write_lock_obj.acquire()
        else:
            self.write_lock_obj.acquire()

    def w_release(self):
        if self.write_lock_obj.locked():
            self.write_lock_obj.release()

    @contextmanager
    def w_locked(self):
        try:
            self.w_acquire()
            yield
        finally:
            self.w_release()
    
    def set_initial_values(self, init):
        self.init = init
    
    def create_local_variable(self, var):
        initial_value = self.init.get(var, 0)
        variable = 0
        ldic = locals()
        try:
            exec(f"variable = self.{var}", ldic)
            # print(f"variable {block['variable']} already exists!")
        except AttributeError:
            exec(f"self.{var} = initial_value")
            exec(f"variable = self.{var}", ldic)
            # print(f"variable {block['variable']} created!")
        finally:
            variable = ldic['variable']
        return variable
    
# Check the concurrency          
class Concurrency:
    def __init__(self, locks):
        self.locks = locks
        
        self.PURPLE = "\033[95m"
        self.CYAN = "\033[96m"
        self.DARKCYAN = "\033[36m"
        self.BLUE = "\033[94m"
        self.GREEN = "\033[92m"
        self.YELLOW = "\033[93m"
        self.RED = "\033[91m"
        self.BOLD = "\033[1m"
        self.END = "\033[0m"
    
    def print(self, sig, statement, *colors):
        cc = ""
        cc = "".join([color for color in colors])
        print()
        print(
            "{mix}[{sig}]{end} {statement}".format(
                sig=sig, mix=cc, end=self.END, statement=statement
            ),
            flush=True
        )
        
    def create_transaction(self, transaction, T_num):
        for block in transaction:
            print()
            self.print("<<<", f"T{T_num}, {block['variable']}", self.GREEN)
            operation_name = block['operations'][0]
            lock_class = self.locks.get(block['variable']) # Fetch variable object from sharedExclusive class.
            if operation_name == 'read_item':
                with lock_class.r_locked():
                    self.print("!!", f"Acquire read_lock for var:{block['variable']!r} of T:{T_num!r}", self.BLUE)
                    variable = lock_class.create_local_variable(block['variable'])
                    self.print("=>", f"{block['variable']}:{variable} of T:{T_num!r}", self.YELLOW)
                    
                    random_number = random.uniform(0.1,1.5)
                    self.print("~~", f"Sleep for {random_number:.2f} s", self.PURPLE)
                    sleep(random_number)
                self.print(">>>", f"Release read_lock for var:{block['variable']!r} of T:{T_num!r}", self.RED)
                    
            elif operation_name == 'write_item':
                operation_task = block['operations'][1]
                with lock_class.w_locked():
                    self.print("!!", f"Acquire write_lock for var:{block['variable']!r} of T:{T_num!r}", self.BLUE)
                    variable = lock_class.create_local_variable(block['variable'])
                    self.print("=>", f"{block['variable']}:{variable} of T:{T_num!r}", self.YELLOW)
                    
                    if len(operation_task.split()) == 1:
                        variable = float(operation_task)
                    else:
                        l_operand, operator, r_operand = operation_task.split()
                        if not (is_float(l_operand) or is_int(l_operand)):
                            l_operand_lock_class = self.locks.get(l_operand)
                            l_operand = l_operand_lock_class.create_local_variable(l_operand)
                            
                        if not (is_float(r_operand) or is_int(r_operand)):
                            r_operand_lock_class = self.locks.get(r_operand)
                            r_operand = r_operand_lock_class.create_local_variable(r_operand)
        
                        mapper = {
                            '+' : _.add,
                            '-' : _.sub,
                            '*' : _.mul,
                            '/' : _.truediv,
                        }
                        func = mapper.get(operator, None)
                        if func:
                            variable = func(float(l_operand), float(r_operand))
                    random_number = random.uniform(0.1,1.5)
                    self.print("~~", f"Sleep for {random_number:.2f} s", self.PURPLE)
                    sleep(random_number)
                    exec(f"lock_class.{block['variable']} = variable")
                self.print(">>>", f"Release write_lock for var:{block['variable']!r} of T:{T_num!r}", self.RED)



def is_float(x):
    try:
        a = float(x)
    except (TypeError, ValueError):
        return False
    else:
        return True

def is_int(x):
    try:
        a = float(x)
        b = int(a)
    except (TypeError, ValueError):
        return False
    else:
        return a == b
    
def create_and_run_threads(schedule, concurrency):
    for index, transaction in enumerate(schedule):
        exec(f"t{index} = Thread(target=concurrency.create_transaction, args=({transaction},{index+1}))")
    index += 1
    for i in range(index):
        exec(f"t{i}.start()")
    for i in range(index):
        exec(f"t{i}.join()")

def get_variables(schedule):
    vars = []
    for transaction in schedule:
        for block in transaction:
            vars.append(block['variable'])
    # Remove duplicates
    return list(set(vars))

def create_lock_objects(vars, SharedExclusive, initial_values):
    objects = {}
    for var in vars:
        ldic = locals()
        exec(f"objects[var] = SharedExclusive()", ldic)
        obj = objects.get(var)
        obj.set_initial_values(initial_values)
    return ldic['objects']

def print_variables(locks):
    print('\nAll used variables and values:')
    for var, obj in locks.items():
        print(var, "=", getattr(obj, var))
        
def main():
    transaction1 = [
        {
            'variable': "y",
            'operations': [
                'read_item', 
            ]
        },
        {
            'variable': "x",
            'operations': [
                'write_item', 
                'x + y'
            ]
        },
    ]
    
    transaction2 = [
        {
            'variable': "x",
            'operations': [
                'read_item', 
            ]
        },
        {
            'variable': "y",
            'operations': [
                'write_item', 
                'x + y'
            ]
        },
    ]
    transaction3 = [
        {
            'variable': "x",
            'operations': [
                'read_item', 
            ]
        },
        {
            'variable': "y",
            'operations': [
                'write_item', 
                'x - 50'
            ]
        },
        {
            'variable': "y",
            'operations': [
                'write_item', 
                'y + 50'
            ]
        },
    ]
    initial_values = {
        'y' : 30,
        'x': 20
    }
    # for simplicity we assume all transactions are same.
    # schedule = [transaction for _ in range(3)]
    schedule = [transaction3, transaction2]
    vars = get_variables(schedule)
    locks = create_lock_objects(vars, SharedExclusive, initial_values)
    concurrency = Concurrency(locks)
    create_and_run_threads(schedule, concurrency)
    print_variables(locks)



main()