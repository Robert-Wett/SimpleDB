__author__ = "Robert Wettlaufer (aka big tasty)"


import sys, os
from collections import deque


class TransactionListEntry(object):
    """
    Helper class to hold necessary information for a single command
    issued by the user.
    """
    def __init__(self, command, key, value, is_new, is_new_temp,
                 previous_value, str_command=None, previous_temp=None):
        self.command = command
        self.key = key
        self.value = value
        self.str_command = str_command
        # `True` if this is an initial SET (no previous value)
        self.is_new = is_new
        self.is_new_temp = is_new_temp
        # Saved to be restored if it existed (ROLLBACK)
        self.previous_value = previous_value
        self.previous_temp = previous_temp

    def command(self):
        return self.command

    def key(self):
        return self.key

    def value(self):
        return self.value

    def is_new(self):
        return self.is_new

    def previous_value(self):
        return self.previous_value


class TransactionList(object):
    def __init__(self):
        self.transaction_list = deque()

    def get_transactions(self):
        return self.transaction_list

    def get_last(self):
        return self.transaction_list[-1]

    def add_transaction(self, transaction_entry):
        self.transaction_list.append(transaction_entry)


class Record(object):
    def __init__(self, value, file_offset):
        self.value = value
        self.file_offset = file_offset

    def value(self):
        return self.value

    def file_offset(self):
        return self.file_offset


class SimpleDB(object):
    def __init__(self, object):
        # Global file pointer that will be opened by load_main()
        self.fp = None
        self.main_db = {}
        self.pending_db = {}
        self.pending_transactions = deque()
        self.prompt = ">> "
        self.current_transaction = None
        self.main_db_path = 'main.sdb'
        self.pending_transactions_log = 'pending.txt'
        # Load main database file
        self.load_main()
        # Check for pending transactions file
        self.prompt_for_reload()

    def prompt_for_reload(self):
        if os.path.exists(self.pending_transactions_log):
            output_str = "The database was shutdown improperly with transactions pending.\n" \
                         "Do you wish to restore pending transactions? (y/n):\n"
            load_pending = input(output_str).upper()
            if len(load_pending) == 0 or load_pending[0] not in ['Y', 'N']:
                print("Please enter `y` for yes, `n` for no.")
                self.prompt_for_reload()
            elif load_pending == 'Y':
                self.load_pending()
            else:
                os.remove(self.pending_transactions_log)

    # Load main database file.  This is only called during initialization and
    # leaves the file open.
    def load_main(self):
        # Create the file if it doesn't exist
        if not os.path.exists(self.main_db_path):
            open(self.main_db_path, 'a').close()
        # Open file for appending and reading, keeping it open.
        self.fp = open(self.main_db_path, "r+")
        self.fp.seek(0)
        # Read in file
        while True:
            # Current file pointer (zero if just opened)
            offset = self.fp.tell()
            input_line = self.fp.readline()
            if not input_line:
                break
            self.process_record(input_line, offset)

    # Process a record read in while loading the file.  If it is marked for deletion,
    # ignore it, otherwise add it to the in-memory database
    def process_record(self, input_line, offset):
        input_record = input_line.strip().split(',')
        # Ignore record if marked as deleted
        if input_record[0] == '1':
            return
        assert isinstance(offset, object)
        self.main_db[input_record[1]] = Record(input_record[2], offset)

    def load_pending(self):
        with open(self.pending_transactions_log) as f:
            command_entries = [x.strip() for x in f.readlines()]
        print("Replaying pending transactions....")
        for command in command_entries:
            self.print_prompt()
            print(command)
            self.process_command(command, prompt=False, in_replay=True)

    def process_command(self, user_input, prompt=True, in_replay=False):
        parsed_command = [x.strip() for x in user_input.split(" ")]
        if len(parsed_command) == 0:
            return
        parsed_command[0] = parsed_command[0].upper()
        command = parsed_command[0]
        if command == "SET":
            self.set(parsed_command, in_replay)
        elif command == 'GET':
            self.get(parsed_command)
        elif command == 'UNSET':
            self.unset(parsed_command, in_replay)
        elif command == 'NUMEQUALTO':
            self.numequalto(parsed_command)
        elif command == 'ROLLBACK':
            self.rollback(in_replay)
        elif command == 'COMMIT':
            self.commit(parsed_command, in_replay)
        elif command == 'BEGIN':
            self.begin(in_replay)
        elif command == 'DISPLAY':
            self.display(parsed_command)
        elif command == 'END':
            # Close main db file
            self.fp.close()
            exit(1)
        elif command == 'CRASH':
            # Simulate a crash
            exit(0)
        if prompt:
            self.print_prompt()

    def set(self, parsed_command, in_replay):
        if len(parsed_command) != 3:
            print("Improper syntax. Usage: `SET {key} {value}`")
            return
        command_string = " ".join(parsed_command)
        command = parsed_command[0]
        key = parsed_command[1]
        try:
            value = int(parsed_command[2])
        except ValueError as e:
            print("Please enter an integer for the value")
            return
        if self.in_transaction():
            self.current_transaction = self.pending_transactions[-1]
            old_value = self.main_db[key].value if key in self.main_db else None
            old_temp = self.pending_db.get(key)
            new_transaction = TransactionListEntry(command, key, value,
                                                   old_value is None,
                                                   old_temp is None,
                                                   old_value, command_string,
                                                   old_temp)
            # Write to transaction log ONLY If not in replay mode
            if not in_replay:
                self.write_log(command_string)
            self.current_transaction.add_transaction(new_transaction)
            self.pending_db[key] = value #Record(value, 0) #new_offset)
        else:
            # Write out to main DB record here
            if key in self.main_db:
                # No sense resetting the existing value
                if self.main_db[key].value == value:
                    return
                self.delete_record(self.main_db[key].file_offset)
            new_offset = self.append_record(key, value)
            self.main_db[key] = Record(value, new_offset)

    def unset(self, parsed_command, in_replay):
        if len(parsed_command) != 2:
            print("Invalid syntax. Usage: `UNSET {key}`")
            return
        command = parsed_command[0]
        key = parsed_command[1]
        if self.in_transaction():
            if key not in self.pending_db:
                return
            command_string = " ".join(parsed_command)
            old_value = self.pending_db[key]
            new_transaction_entry = TransactionListEntry(command, key, None, None,
                                                         None, old_value is None,
                                                         str_command=command_string)
            self.current_transaction = self.pending_transactions[-1]
            self.current_transaction.add_transaction(new_transaction_entry)
            del self.pending_db[key]
            if not in_replay:
                self.write_log(command_string)
        else:
            if key not in self.main_db:
                return
            # `Delete` or mark the record as invalid in the `.sdb` file
            self.delete_record(self.main_db[key].file_offset)
            # Remove entry from in-memory database
            del self.main_db[key]

    def get(self, parsed_command):
        pending = False
        if len(parsed_command) not in (2, 3):
            print("Invalid syntax. Usage: `GET {key} PENDING`")
            return
        if len(parsed_command) == 3:
            pending = True
        command = parsed_command[0]
        key = parsed_command[1]
        if pending:
            print(self.pending_db[key] if key in self.pending_db else "Nothing pending")
        else:
            print(self.main_db[key].value if key in self.main_db else "NULL")

    def numequalto(self, parsed_command):
        if len(parsed_command) != 2:
            print("Invalid syntax. Usage: `numequalto {value}`")
            return
        try:
            value = int(parsed_command[1])
        except ValueError as e:
            print("Please enter an integer for the value")
            return
        num_equal = [v for k, v in self.main_db.items() if int(v.value) == value]
        print(len(num_equal))

    def commit(self, parsed_command, in_replay):
        # Commit all key/values in the pending transactions DB to the main DB
        if len(parsed_command) == 2 and parsed_command[1][0].upper() == 'A':
            self.commit_all()
        else:
            # We have at least 1 transaction pending
            if len(self.pending_transactions) > 0:
                self.current_transaction = self.pending_transactions.pop()
                transactions = self.current_transaction.get_transactions()
                while len(transactions) > 0:
                    entry = transactions.pop()
                    if entry.command == 'SET':
                        self.pending_db[entry.key] = entry.previous_temp
                        file_offset = self.append_record(entry.key, entry.value)
                        self.main_db[entry.key] = Record(entry.value, file_offset)
                    elif entry.command == 'UNSET' and entry.key in self.main_db:
                        self.main_db[entry.key].value = entry.previous_temp
                if not in_replay:
                    self.write_log("COMMIT")
                # If this was the only/last transaction block, then call the
                # commit_all method to dispose of the temp file
                if len(self.pending_transactions) == 0:
                    self.commit_all()
            else:
                # No pending transactions, write everything to the main DB
                self.commit_all()
        return

    def commit_all(self):
        self.main_db.update(self.pending_db)
        self.pending_transactions.clear()
        self.pending_db.clear()
        # Get rid of the transaction file - all transactions committed.
        if os.path.exists(self.pending_transactions_log):
            os.remove(self.pending_transactions_log)

    def display(self, parsed_command):
        if len(parsed_command) == 1:
            if len(self.main_db) > 0:
                print("\nCurrent Entries in Database")
                print("===========================")
                for key, record in self.main_db.items():
                    print(key, record.value)
                print()
            else:
                print("\nDatabase is empty.\n")
        # check for `pending` switch, or just `p`
        elif len(parsed_command) == 2 and parsed_command[1][0].upper() == 'P':
            if len(self.pending_transactions) > 0:
                print('\nPending Transaction List\n========================')
                for idx, trans in enumerate(self.pending_transactions):
                    print("    " * idx + "BEGIN")
                    for entry in trans.transaction_list:
                        print("    " * (idx + 1) + entry.str_command)
            else:
                print('\nNo Pending Transactions\n')

    def rollback(self, in_replay):
        if len(self.pending_transactions) == 0:
            print("NO TRANSACTION")
            return
        current_transaction = self.pending_transactions.pop()
        transactions = current_transaction.get_transactions()
        while len(transactions) - 1 >= 0:
            entry = transactions.pop()
            if entry.command == 'SET':
                # If there was no previous value in the temp DB:
                if entry.is_new_temp and entry.key in self.pending_db:
                    del self.pending_db[entry.key]
                else:
                    self.pending_db[entry.key] = entry.previous_temp
            elif entry.command == 'UNSET' and entry.key in self.main_db:
                self.main_db[entry.key].value = entry.previous_value
        # If this is the only transaction block we have, then all values
        # are in the main DB. Call `commit_all` to dispose of the temp file.
        # This can be done better.
        if len(self.pending_transactions) == 0:
            self.commit_all()
        elif not in_replay:
            self.write_log("ROLLBACK")

    def begin(self, in_replay):
        self.pending_transactions.append(TransactionList())
        if not in_replay:
            self.write_log("BEGIN")

    #  Marks an existing database record as deleted
    def delete_record(self, offset):
        self.fp.seek(offset)
        # Mark as deleted
        self.fp.write("1")
        # Flush process address space buffers to OS buffers
        self.fp.flush()
        # Courtesy flush (tell OS to flush writes to disk)
        os.fsync(self.fp.fileno())

    #  Appends a record to the end of the database file.
    #  Comma-delimited record (text) format:
    #       `0,a,100\n`
    #       One byte string (either "0" for not deleted or "1" for deleted)
    #       key_string
    #       value_string
    #
    def append_record(self, key, value):
        # Seek to the end of the file
        self.fp.seek(0, 2)
        # Get the offset
        offset = self.fp.tell()
        # Set the record to be `valid`, aka, not deleted
        self.fp.writelines("0," + key + ',' + str(value) + '\n')
        # Flush process address space buffers to OS buffers
        self.fp.flush()
        # Tell OS to flush writes to disk
        os.fsync(self.fp.fileno())
        assert isinstance(offset, int)
        # Return the offset to be committed to the in-memory DB
        return offset

    def write_log(self, str_command):
        """
        Write user command to the pending transactions file
        """
        with open(self.pending_transactions_log, mode='a') as f:
            f.write(str_command+'\n')
            f.flush()
            os.fsync(f.fileno())

    def in_transaction(self):
        return len(self.pending_transactions) > 0

    def print_prompt(self):
        sys.stdout.write(self.prompt + "   " * len(self.pending_transactions))
        sys.stdout.flush()

if __name__ == '__main__':
    db = SimpleDB()
    db.print_prompt()
    # Start the input loop
    for line in sys.stdin:
        db.process_command(line)
