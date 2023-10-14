from threading import Thread, Lock
import random
import operator as _
from time import sleep
from contextlib import contextmanager

class BinaryLock(object):
    def __init__(self):
        self.lock_item_obj = Lock()
        self.x = 0
    
    @contextmanager
    def lock_item(self):
        print("\nlock item called:")
        if self.lock_item_obj.locked():
            print("\nWait for locked item")
                # waiting until write_lock released...
            self.lock_item_obj.acquire()
            # while self.lock_item_obj.locked():
            #     print(".",end="")
        else:
            # print("\nAcquire lock_item")
            self.lock_item_obj.acquire()
                
    def lock_release(self):
        # print("\nRelease lock")
        if self.lock_item_obj.locked():
            self.lock_item_obj.release()

    @contextmanager
    def locking_item(self):
        try:
            self.lock_item()
            yield
        finally:
            self.lock_release()

    def create_local_variable(self, block):
        variable = 0
        ldic = locals()
        try:
            exec(f"variable = self.{block['variable']}", ldic)
            # print(f"variable {block['variable']} already exists!")
        except AttributeError:
            exec(f"self.{block['variable']} = 0")
            exec(f"variable = self.{block['variable']}", ldic)
            # print(f"variable {block['variable']} created!")
        finally:
            variable = ldic['variable']
        return variable
    

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
            )
        )
    def create_transaction(self, transaction, T_num):
        for block_variable in transaction:
            var = block_variable[0]['variable']
            print()
            self.print("<<<", f"T{T_num}, {var}", self.GREEN)
            lock_class = self.locks.get(var) # Fetch variable object from sharedExclusive class.
            with lock_class.locking_item():
                self.print("!!", f"Acquire lock for var:{var!r} of T:{T_num!r}", self.BLUE)
                variable = lock_class.create_local_variable(block_variable[0])
                print(f"{var}:{variable}")
                random_number = random.uniform(0.1,1.5)
                print(f"~~ Sleep for {random_number:.2f} s")
                sleep(random_number)
                for operation in block_variable:
                    variable = lock_class.create_local_variable(operation)
                    op_task = operation['operations']
                    if op_task[0] == 'write_item':
                        operator, operand = op_task[1].split()
                        mapper = {
                            '+' : _.add,
                            '-' : _.sub,
                            '*' : _.mul,
                            '/' : _.truediv,
                        }
                        func = mapper.get(operator, None)
                        if func:
                            variable = func(variable, float(operand))
                    random_number = random.uniform(0.1,1.5)
                    self.print("=>", f"{var}:{variable} of T:{T_num!r}", self.YELLOW)
                    self.print("~~", f"Sleep for {random_number:.2f} s", self.PURPLE)
                    sleep(random_number)
                    exec(f"lock_class.{var} = variable")
                self.print(">>>", f"Release lock for var:{var!r} of T:{T_num!r}", self.RED)



def create_and_run_threads(schedule, concurrency):
    for index, transaction in enumerate(schedule):
        exec(f"t{index} = Thread(target=concurrency.create_transaction, args=({transaction},{index+1}))")
    index += 1
    for i in range(index):
        exec(f"t{i}.start()")
    for i in range(index):
        exec(f"t{i}.join()")
def sort_transaction(transaction):
    T_vars = []
    sorted= []
    for item in transaction:
        if item['variable'] not in T_vars:
            T_vars.append(item['variable'])

    for i in T_vars:
        temp = []
        for item in transaction:
            if item['variable'] == i:
                temp.append(item)
        sorted.append(temp)
    return sorted

def get_variables(schedule):
    vars = []
    for transaction in schedule:
        for block in transaction:
            vars.append(block['variable'])
    # Remove duplicates
    return list(set(vars))

def create_lock_objects(vars, BinaryLock):
    objects = {}
    for var in vars:
        ldic = locals()
        exec(f"objects[var] = BinaryLock()", ldic)
    return ldic['objects']

def print_variables(locks):
    print('\nAll used variables and values:')
    for var, obj in locks.items():
        print(var, "=", getattr(obj, var))
        
def main():
    transaction = [
        {
            'variable': "y",
            'operations': [
                'read_item', 
            ]
        },
        {
            'variable': "x",
            'operations': [
                'read_item', 
            ]
        },
        {
            'variable': "x",
            'operations': [
                'write_item', 
                '+ 10'
            ]
        },
        {
            'variable': "y",
            'operations': [
                'write_item', 
                '+ 10'
            ]
        },
    ]
    # for simplicity we assume all transactions are same.
    schedule = [transaction for _ in range(2)]
    vars = get_variables(schedule)
    locks = create_lock_objects(vars, BinaryLock)
    concurrency = Concurrency(locks)
    transaction = sort_transaction(transaction)
    schedule = [transaction for _ in range(2)]
    create_and_run_threads(schedule, concurrency)
    print_variables(locks)



main()