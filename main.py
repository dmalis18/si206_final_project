import unittest
from pybaseball import amateur_draft
from pybaseball import amateur_draft_by_team
import sqlite3
import csv
import numpy as np
import json
import os
import requests

def populate_teams(cur, conn):
    pass

def populate_if_team_drafted_data(cur, conn):
    pass

def populate_team_draft_data(cur, conn):
    pass

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

def set_up_team_draft_data(cur, conn):
    cur.execute(
        "CREATE TABLE IF NOT EXISTS DRAFTED_BY_TEAM (id INTEGER PRIMARY KEY, draft_pick INTEGER, name TEXT)"
    )
    conn.commit()

def set_up_if_team_drafted_data(cur, conn):
    cur.execute(
        "CREATE TABLE IF NOT EXISTS TEAM_DRAFTED (id INTEGER PRIMARY KEY, draft_pick INTEGER, name TEXT)"
    )
    conn.commit()

def set_up_teams(cur, conn):
    cur.execute(
        "CREATE TABLE IF NOT EXISTS TEAMS(id INTEGER PRIMARY KEY, team_name TEXT)"
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
    set_up_team_draft_data(cur, conn)
    set_up_if_team_drafted_data(cur, conn)
    set_up_teams(cur, conn)
    # get_all_needed_draft_data()
    # read_active_teams(1990, 2015, cur, conn)
    populate_teams(cur, conn)
    populate_if_team_drafted_data(cur, conn)
    populate_team_draft_data(cur, conn)

    # Close database connection
    conn.close()

main()
