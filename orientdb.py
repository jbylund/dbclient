#!/usr/bin/python
import argparse
from getpass import getuser
import json
import os
import readline  # not to be confused with the builtin
import pyorient


def toserial(obj):
    try:
        json.dumps(obj)
        return obj
    except BaseException:
        return str(obj)


class CLI(object):
    def __init__(self, **kwargs):
        for key, val in kwargs.iteritems():
            setattr(self, key, val)
        self.client = None
        self.session_id = None
        self.cmdno = 0
        self._connect()
        readline.set_completer(self._completion)
        readline.parse_and_bind('tab: complete')

    def _completion(self, needle, state):
        needle = needle.upper()
        keywords = [
            "CREATE",
            "DELETE",
            "EOF",
            "FETCHPLAN",
            "GROUP",
            "INSERT",
            "LET",
            "LIMIT",
            "LOCK",
            "NOCACHE",
            "OFFSET",
            "ORDER",
            "PARALLEL",
            "SELECT",
            "SKIP2",
            "TIMEOUT",
            "UNWIND",
            "UPDATE",
            "WHERE",
        ]
        matches = [x for x in keywords if x.startswith(needle)]
        try:
            return matches[state] + " "
        except BaseException:
            return  # non-string return value means we're done

    def _connect(self):
        self.cmdno = 0
        try:
            self.client = pyorient.OrientDB(self.host, self.port)
        except BaseException:
            raise Exception(
                'Could not connect to {}:{} is the server running & listening on that port?'
            )
        try:
            self.session_id = self.client.connect(self.username, self.password)
        except BaseException:
            raise Exception('Could not connect as user, check credentials.')
        try:
            self.client.db_open(
                self.dbname,
                self.username,
                self.password
            )
        except BaseException:
            raise Exception('Could not open database, check database and credentials.')

    def _read(self):
        self.cmdno += 1
        prompt = "In [{}]: ".format(self.cmdno)
        return raw_input(prompt).strip()

    def _eval(self, cmd):
        res = self.client.command(cmd)
        return [row.oRecordData for row in res]

    def _print(self, res):
        print json.dumps(res, indent=4, sort_keys=True, default=toserial)
        print

    class UnkownMetaCommand(Exception):
        pass

    def _special_cmd(self, cmd):
        cmd = cmd.lstrip('\\')
        if cmd == 'dt':
            return [
                row.oRecordData for row in
                self.client.command('select from (select expand(classes) from metadata:schema)')
            ]
        else:
            raise self.UnkownMetaCommand('Unknown meta command!')

    def repl(self):
        """read / eval / print"""
        while True:
            try:
                cmd = self._read()
                if cmd[0] == '\\':
                    res = self._special_cmd(cmd)
                else:
                    res = self._eval(cmd)
                self._print(res)
            except self.UnkownMetaCommand:
                print "Unkown meta command '{}'.".format(cmd)
                print
            except (pyorient.exceptions.PyOrientSQLParsingException, pyorient.exceptions.PyOrientCommandException) as oops:
                print oops
                print
            except (KeyboardInterrupt, EOFError):
                print
                print
                break


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dbname', default=os.environ.get('ODBDBNAME') or getuser())
    parser.add_argument('--username', default=os.environ.get('ODBUSER') or getuser())
    parser.add_argument('--host', default=os.environ.get('ODBHOST') or 'localhost')
    default_port = 2424
    if os.environ.get('ODBPORT'):
        default_port = int(os.environ.get('ODBPORT'))
    parser.add_argument('--port', type=int, default=default_port)
    parser.add_argument('--password', default=os.environ.get('ODBPASSWORD'))
    return vars(parser.parse_args())


def main():
    args = get_args()
    CLI(**args).repl()


if "__main__" == __name__:
    main()
