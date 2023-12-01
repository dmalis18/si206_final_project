import unittest
from pybaseball import amateur_draft
from pybaseball import bwar_bat
from pybaseball import bwar_pitch
from pybaseball import amateur_draft_by_team
import sqlite3
import json
import os

def read_round_draft_data_specific_year(year, round, cur, conn):
    """
    Reads data from a file with the given filename.

    Parameters
    -----------------------
    filename: str
        The name of the file to read.

    Returns
    -----------------------
    dict:
        Parsed JSON data from the file.
    """
    draftResults = amateur_draft(year, round)
        # Grab OvPck, Name, Pos, Tm
    for i in range(len(draftResults)):
        ovPck = draftResults["OvPck"][i]
        name = draftResults["Name"][i]
        pos = draftResults["Pos"][i]
        tm = draftResults["Tm"][i]
        primary_key = int(str(year) + str(round) + str(ovPck))
        cur.execute("INSERT OR IGNORE INTO DRAFTED_BY_ROUND (id, name, position, team, year, round) VALUES (?, ?, ?, ?, ?, ?)", (primary_key, name, pos, tm, year, round))
    conn.commit()
    for item in cur.execute("SELECT * FROM DRAFTED_BY_ROUND"):
        print(item)

def read_team_draft_data(team_name, year, cur, conn):
    draft_results = amateur_draft_by_team(team_name, year)
    for i in range(len(draft_results)):
        # OvPck, Tm, Year, Round
        OvPck = draft_results["OvPck"][i]
        name = draft_results["Name"][i]
        Tm = draft_results["Tm"][i]
        Round = draft_results["Round"][i]
        pos = draft_results["Pos"][i]
        tm = draft_results["Tm"][i]
        primary_key = int(str(year) + str(Round) + str(OvPck))
        cur.execute("INSERT OR IGNORE INTO DRAFTED_BY_ROUND (id, name, position, team, year, round) VALUES (?, ?, ?, ?, ?, ?)", (primary_key, name, pos, tm, year, round))
    conn.commit()

def set_up_database(db_name):
    """
    Sets up a SQLite database connection and cursor.

    Parameters
    -----------------------
    db_name: str
        The name of the SQLite database.

    Returns
    -----------------------
    Tuple (Cursor, Connection):
        A tuple containing the database cursor and connection objects.
    """
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + "/" + db_name)
    cur = conn.cursor()
    return cur, conn


def set_up_player_draft_data(cur, conn):
    """
    Sets up the Types table in the database using the provided Pokemon data.

    Parameters
    -----------------------
    data: list
        List of Pokemon data in JSON format.

    cur: Cursor
        The database cursor object.

    conn: Connection
        The database connection object.

    Returns
    -----------------------
    None
    """
    cur.execute(
        "CREATE TABLE IF NOT EXISTS DRAFTED_BY_ROUND (id INTEGER PRIMARY KEY, name TEXT, position TEXT, team TEXT, year INTEGER, round INTEGER)"
    )
    conn.commit()

def set_up_team_draft_data(cur, conn):
    cur.execute(
        "CREATE TABLE IF NOT EXISTS DRAFTED_BY_TEAM (id INTEGER PRIMARY KEY, draft_pick INTEGER, name TEXT)"
    )
    conn.commit()

def main():
    """
    Main execution function.

    Parameters
    -----------------------
    None

    Returns
    -----------------------
    None
    """

    # Set up database
    cur, conn = set_up_database("baseball.db")
    set_up_player_draft_data(cur, conn)
    #read_round_draft_data_specific_year(2017, 3, cur, conn)
    read_team_draft_data("TBD", 2011, cur, conn)
    #get_bwar_data()

    # Close database connection
    conn.close()

main()
