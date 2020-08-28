#!/usr/bin/python
import argparse
from getpass import getuser
import json
import os
import readline  # not to be confused with the builtin
import sys
import time
import sqlite3
import logging
import traceback


logger = logging.getLogger(__file__ if __name__ == "__main__" else __name__)

def toserial(obj):
    try:
        json.dumps(obj)
        return obj
    except:
        return str(obj)


class CLI(object):
    def __init__(self, **kwargs):
        for key, val in kwargs.items():
            setattr(self, key, val)

        self.cmdno = 0
        self.tablenames = None
        self.timing = False
        self.connection = sqlite3.connect(self.database)
        self.connection.row_factory = sqlite3.Row
        self.cursor = self.connection.cursor()

        readline.set_completer(self._completion)
        readline.parse_and_bind('tab: complete')
        self.keywords = set([
            "ALTER",
            "AND",
            "ATTACH",
            "BEGIN",
            "BY",
            "COMMIT",
            "CONFLICT",
            "CREATE",
            "DATABASE",
            "DELETE",
            "DETACH",
            "DROP",
            "END",
            "EXPLAIN",
            "FROM",
            "HAVING",
            "INDEX",
            "INDEXED",
            "INNER",
            "INSERT",
            "JOIN",
            "LEFT",
            "LIMIT",
            "ON",
            "OR",
            "OUTER",
            "PRAGMA",
            "REINDEX",
            "RELEASE",
            "REPLACE",
            "RIGHT",
            "ROLLBACK",
            "SAVEPOINT",
            "SELECT",
            "TABLE",
            "TRANSACTION",
            "TRIGGER",
            "UPDATE",
            "UPSERT",
            "VACUUM",
            "VIEW",
            "VIRTUAL",
            "WHERE",
            "WITH",
        ])

    def query_has(self, keyword):
        current_words = readline.get_line_buffer().strip().lower().split()
        return keyword.lower() in current_words

    def query_ends_with(self, keyword):
        current_words = readline.get_line_buffer().strip().lower().split()
        return current_words[-1] == keyword.lower()


    def _completion(self, needle, state):
        try:
            return self.__completion(needle, state)
        except:
            print(traceback.format_exc())
            raise

    def get_last_keyword(self):
        cur_words = self.get_current_words()
        while cur_words:
            if cur_words[-1] in self.keywords:
                return cur_words[-1]
            cur_words.pop()

    def get_current_words(self):
        return (readline.get_line_buffer() + "|").upper().split()


    def __completion(self, needle, state):
        current_text = readline.get_line_buffer()
        needle = needle.upper()
        matches = []
        current_words = self.get_current_words()
        last_keyword = self.get_last_keyword() or ""
        if 2 < len(current_words) and "FROM" == current_words[-2]:
            # match from tablenames
            matches = [
                tablename
                for tablename in self._get_tablenames()
                if tablename.upper().startswith(needle)
            ]
        elif 2 < len(current_words) and "WHERE" == current_words[-2]:
            # autocomplete to columns in tables that are mentioned
            matches = set()
            for itable in self._get_tablenames():
                if itable.upper() in current_words:
                    matches.update(c.upper() for c in self.get_columns_for_table(itable))
            matches = sorted(c for c in matches if c.startswith(needle))
        else:
            matches = [x for x in self.keywords if x.startswith(needle)]
        try:
            return matches[state] + " "
        except IndexError:
            return  # non-string return value means we're done

    def get_columns_for_table(self, table):
        self.cursor.execute(f"SELECT * FROM {table} WHERE FALSE")
        return (c[0] for c in self.cursor.description)

    def _get_in_txn_str(self):
        if self.connection.in_transaction:
            return "(in txn) "
        return ""

    def _read(self):
        self.cmdno += 1
        pieces = []
        while not pieces or not pieces[-1].endswith(';'):
            nprompt = "{dbname} {in_txn}=> ".format(
                dbname=os.path.basename(self.database).partition('.')[0],
                in_txn=self._get_in_txn_str(),
            )
            cprompt = "...: ".rjust(len(nprompt))
            if not pieces:
                prompt = nprompt
            else:
                prompt = cprompt
            pieces.append(input(prompt).strip())
            if pieces[0].startswith('\\'):
                break
        return " ".join(pieces)

    def _eval(self, cmd):
        return [dict(row) for row in self.cursor.execute(cmd)]

    def _print(self, res, duration=0):
        if res is not None:
            print(json.dumps(res, indent=4, sort_keys=True, default=toserial))
            print("({} row{})".format(
                len(res),
                "" if 1 == len(res) else "s"
            ))
        if self.timing:
            print("\nTime: {} ms".format(round(1000 * duration, 3)))
        print()

    class UnkownMetaCommand(Exception):
        pass

    def _get_tablenames(self):
        if self.tablenames is None:
            self.tablenames = sorted(
                row['name'] for row in
                self._eval('SELECT name from sqlite_master where type= "table" order by name')
            )
        return self.tablenames

    def do_table_info(self, args):
        try:
            tablename = args[0]
        except IndexError:
            print("Missing required parameter tablename")
            return
        pragma_info = self._eval('PRAGMA TABLE_INFO({})'.format(tablename))
        print("""Table "{}" """.format(tablename).center(62))
        print("|".join(x.center(20) for x in ["Column", "Type", "Nullable"]))
        print("|".join([20 * "-"] * 3))
        for colinfo in pragma_info:
            print("{colname}|{coltype}|{nullable}".format(
                colname=colinfo.get("name").center(20),
                coltype=colinfo.get("type").lower().center(20),
                nullable=("not null" if colinfo.get("notnull") else "").center(20),
            ))

    def _special_cmd(self, cmd):
        cmd = cmd.lstrip('\\')
        args = cmd.split()[1:]
        cmd = cmd.split()[0]
        if cmd == 'dt':
            return self._eval('SELECT name from sqlite_master where type= "table" order by name')
        elif cmd == 'd+':
            return self.do_table_info(args)
        elif cmd == 'd':
            return self.do_table_info(args)
        elif cmd == 'timing':
            self.timing = not(self.timing) # it's a toggle
        else:
            raise self.UnkownMetaCommand('Unknown meta command!')

    def repl(self):
        """read / eval / print"""
        while True:
            try:
                cmd = self._read()
                begin = time.time()
                if not cmd:
                    res = None
                elif cmd[0] == '\\':
                    res = self._special_cmd(cmd)
                else:
                    res = self._eval(cmd)
                end = time.time()
                self._print(res, duration=end-begin)
            except self.UnkownMetaCommand:
                print("Unkown meta command '{}'.".format(cmd))
                print()
            except (KeyboardInterrupt, EOFError):
                print()
                print()
                break


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('database')
    return vars(parser.parse_args())


def main():
    args = get_args()
    logging.basicConfig(level=logging.INFO)
    CLI(**args).repl()


if "__main__" == __name__:
    main()
