import unittest
from pybaseball import amateur_draft
from pybaseball import bwar_bat
from pybaseball import bwar_pitch
from pybaseball import amateur_draft_by_team
import sqlite3
import csv
import numpy as np
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
    #     cur.execute("INSERT OR IGNORE INTO DRAFTED_BY_ROUND (id, name, position, team, year, round) VALUES (?, ?, ?, ?, ?, ?)", (primary_key, name, pos, tm, year, round))
    # conn.commit()
    for item in cur.execute("SELECT * FROM DRAFTED_BY_ROUND"):
        print(item)

def read_team_draft_data(team_name, year):
    draft_results = amateur_draft_by_team(team_name, year)

    with open('draft_data.txt', 'a', newline='') as csvfile:
        fieldnames = ['id', 'Year', 'OvPck', 'Name', 'Tm', 'Round', 'Pos', 'G']  # Add the field names as per your data
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        for i in range(len(draft_results)):
            # OvPck, Tm, Year, Round
            OvPck = draft_results["OvPck"][i]
            name = draft_results["Name"][i]
            tm = draft_results["Tm"][i]
            pos = draft_results["Pos"][i]
            games = draft_results["G"][i]
            signed = draft_results["Signed"][i]
            if signed == "N":
                continue
            if np.isnan(games):
                games = False
            else:
                games = True

            primary_key = int(str(year) + str(OvPck))

            # Write the data to the CSV file
            writer.writerow({'OvPck': OvPck, 'Year': year, 'Name': name, 'Tm': tm, 'Pos': pos, 'G': games, 'id': primary_key})

    print("Data has been written to draft_data.txt")

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

def get_all_needed_draft_data():
    teams = ['ANA','HOU','OAK','TOR','ATL','MIL', 'STL','CHC','TBD','ARI','LAD','SFG','CLE','SEA','FLA','NYM','WSN','BAL','SDP','PHI','PIT','TEX','BOS','CIN','COL','KCR','DET','MIN','CHW','NYY']
    with open('draft_data.txt', 'w', newline='') as csvfile:
        fieldnames = ['id', 'Year', 'OvPck', 'Name', 'Tm', 'Round', 'Pos', 'G']  # Add the field names as per your data
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
    for team in teams:
        for year in range(1990, 2016):
            try:
                read_team_draft_data(team, year)
            except:
                print(team, year)
                

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
    # get_all_needed_draft_data()

    # Close database connection
    conn.close()

main()
