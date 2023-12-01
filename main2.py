import unittest
from pybaseball import amateur_draft
from pybaseball import bwar_bat
from pybaseball import bwar_pitch
from pybaseball import amateur_draft_by_team
import requests
import sqlite3
import json
import os
import copy

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

def read_team_draft_data(team_name, year):
    rays_2011_draft = amateur_draft_by_team("TBD", 2011)
    for i in range(len(rays_2011_draft)):
        # OvPck, Tm, Year, Round
        OvPck = rays_2011_draft["OvPck"][i]
        Tm = rays_2011_draft["Tm"][i]
        Round = rays_2011_draft["Round"][i]
        primary_key = int(str(year) + str(Round) + str(OvPck))
        print(primary_key)

def read_active_teams(start_year:int, end_year:int, cur, conn):
    # build set of all teams from 2022
    all_teams_set = set()
    resp = requests.get(f"http://lookup-service-prod.mlb.com/json/named.team_all_season.bam?sport_code='mlb'&all_star_sw='N'&sort_order=name_asc&season='2022'")
    data = resp.json()
    i = 0
    for team in data['team_all_season']['queryResults']['row']:
        all_teams_set.add(team['mlb_org'])
        i += 1
        
    # build set of teams that existed in the start year
    teams = set()
    year = f"'{str(start_year)}'"
    resp = requests.get(f"http://lookup-service-prod.mlb.com/json/named.team_all_season.bam?sport_code='mlb'&all_star_sw='N'&sort_order=name_asc&season={year}")
    data = resp.json()
    for team in data['team_all_season']['queryResults']['row']:
        teams.add(team['mlb_org'])
    
    # print to csv if team has existed since start year til 2022
    with open("valid_teams.txt", "w") as file:
        file.write("Team,Valid\n")
        for team in all_teams_set:
            if team in all_teams_set.intersection(teams):
                file.write(f"{team},True\n")
            else:
                file.write(f"{team},False\n")


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

def set_up_valid_team_table(cur, conn):
    cur.execute(
        "CREATE TABLE IF NOT EXISTS VALID_TEAM (team TEXT PRIMARY KEY, valid BOOLEAN)"
    )

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
    set_up_valid_team_table(cur, conn)
    #read_round_draft_data_specific_year(2017, 3, cur, conn)
    #read_team_draft_data("TBD", 2011)
    #get_bwar_data()
    read_active_teams(1990, 2015, cur, conn)

    # Close database connection
    conn.close()

main()
