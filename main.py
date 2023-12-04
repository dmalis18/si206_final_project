import unittest
from pybaseball import amateur_draft
from pybaseball import amateur_draft_by_team
import sqlite3
import csv
import numpy as np
import json
import os
import requests
import matplotlib.pyplot as plt

def fill_teams_by_25(cur, conn):
    # Read the team numbers from the CSV file
    with open('team_number.csv', 'r') as csvfile:
        # Create a CSV reader
        csv_reader = csv.DictReader(csvfile)

        # Iterate through each row in the CSV file
        get_count = cur.execute("SELECT COUNT(*) FROM TEAMS")
        val = (get_count.fetchone()[0])
        counter = 0
        for row in csv_reader:
            if counter < int(val):
                counter += 1
                continue
            # Insert the team name and number into the database
            cur.execute("INSERT OR IGNORE INTO TEAMS (team_name, id) VALUES (?, ?)", (row['Team'],int(row['Number'])))
            counter += 1
            if counter >= int(val) + 25:
                break
        conn.commit()
        return counter - val

def fill_valid_teams_by_25(cur, conn):
    # Read the team numbers from the CSV file
    with open('valid_teams.csv', 'r') as csvfile:
        # Create a CSV reader
        csv_reader = csv.DictReader(csvfile)

        # Iterate through each row in the CSV file
        get_count = cur.execute("SELECT COUNT(*) FROM TEAM_DRAFTED")
        val = (get_count.fetchone()[0])
        counter = 0
        for row in csv_reader:
            if counter < int(val):
                counter += 1
                continue
            print(row['Valid'])
            if row['Valid'] == 'False':
                row['Valid'] = False
            else:
                row['Valid'] = True
            # Insert the team name and number into the database
            counter += 1
            cur.execute("INSERT OR IGNORE INTO TEAM_DRAFTED (team_id, valid) VALUES (?, ?)", (counter,row['Valid']))
            if counter >= int(val) + 25:
                break
        conn.commit()
        return counter - val
    
def fill_team_draft_data_by_25(cur, conn, teams_dict):
    # Read the team numbers from the CSV file
    with open('draft_data.csv', 'r') as csvfile:
        # Create a CSV reader
        csv_reader = csv.DictReader(csvfile)

        # Iterate through each row in the CSV file
        get_count = cur.execute("SELECT COUNT(*) FROM DRAFTED_BY_TEAM")
        val = (get_count.fetchone()[0])
        counter = 0
        for row in csv_reader:
            if counter < int(val):
                counter += 1
                continue
            if row['G'] == 'False':
                row['G'] = False
            else:
                row['G'] = True
            # Insert the team name and number into the database
            cur.execute("INSERT OR IGNORE INTO DRAFTED_BY_TEAM (id, overall_pick, year, name, team_id, reached_majors) VALUES (?, ?, ?, ?, ?, ?)", (row['id'], row['OvPck'], row['Year'], row['Name'], teams_dict[row['Tm']], row['G']))
            counter += 1
            if counter >= int(val) + 25:
                break
        conn.commit()
        return counter - val

def populate_teams(cur, conn):
    with open('valid_teams.csv', 'r') as csvfile:
        # Create a CSV reader
        csv_reader = csv.DictReader(csvfile)

        # Open the output file for writing
        with open('team_number.csv', 'w') as outfile:
            # Write header to the file
            outfile.write("Team,Number\n")

            # Counter to assign numbers
            team_counter = 1

            # Iterate through each row in the CSV file
            for row in csv_reader:
                # Write team name and its corresponding number to the file
                outfile.write(f"{row['Team']},{team_counter}\n")

                # Increment the counter
                team_counter += 1
    val = 1
    while(val != 0):
        val = fill_teams_by_25(cur, conn)
        print(f"Filled {val} teams")
    print("Finished filling teams")
    

def populate_if_team_drafted_data(cur, conn):
    # For each row in valid_teams.csv check if the valid team is True/False and insert to table
    val = -1
    while(val != 0):
        val = fill_valid_teams_by_25(cur, conn)
        print(f"Filled {val} teams")
    print("Finished filling teams")

def populate_team_draft_data(cur, conn):
    val = -1
    teams_dict = {}
    cur.execute("SELECT * FROM TEAMS")
    for row in cur:
        teams_dict[row[1]] = row[0]
    print(teams_dict)
    while(val != 0):
        val = fill_team_draft_data_by_25(cur, conn, teams_dict)
        print(f"Filled {val} players")
    print("Finished filling players")
    print("Size of DRAFTED_BY_TEAM: ", cur.execute("SELECT COUNT(*) FROM DRAFTED_BY_TEAM").fetchone()[0])

def read_team_draft_data(team_name, year):
    draft_results = amateur_draft_by_team(team_name, year)

    with open('draft_data.csv', 'a', newline='') as csvfile:
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
        "CREATE TABLE IF NOT EXISTS DRAFTED_BY_TEAM (id INTEGER PRIMARY KEY, overall_pick INTEGER, year Integer, name TEXT, team_id INTEGER, reached_majors BOOLEAN, FOREIGN KEY(team_id) REFERENCES TEAMS(id))"
    )
    conn.commit()

def set_up_if_team_drafted_data(cur, conn):
    cur.execute(
        "CREATE TABLE IF NOT EXISTS TEAM_DRAFTED (team_id INTEGER PRIMARY KEY, valid BOOLEAN)"
    )
    conn.commit()

def set_up_teams(cur, conn):
    cur.execute(
        "CREATE TABLE IF NOT EXISTS TEAMS(id INTEGER PRIMARY KEY, team_name TEXT)"
    )
    conn.commit()

def get_all_needed_draft_data():
    teams = ['ANA','HOU','OAK','TOR','ATL','MIL', 'STL','CHC','TBD','ARI','LAD','SFG','CLE','SEA','FLA','NYM','WSN','BAL','SDP','PHI','PIT','TEX','BOS','CIN','COL','KCR','DET','MIN','CHW','NYY']
    with open('draft_data.csv', 'w', newline='') as csvfile:
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

def get_number_draft_picks_reach_majors(cur, conn):
    file = open("draft_pick_success.csv", 'w')
    file.write("OverallPick,TotalPicks,ReachedMajors\n")
    for i in range(1, 301):
        reach_majors = cur.execute("SELECT COUNT(*) FROM DRAFTED_BY_TEAM WHERE overall_pick = ? AND reached_majors = TRUE", (i,)).fetchone()[0]
        total_picks = cur.execute("SELECT COUNT(*) FROM DRAFTED_BY_TEAM WHERE overall_pick = ?", (i,)).fetchone()[0]
        print(f"Overall Pick: {i}")
        print(f"Total Picks: {total_picks}")
        print(f"Reach Majors: {reach_majors}")

        file.write(f"{i},{total_picks},{reach_majors}\n")

    file.close()

def get_team_success_rate(cur, conn):
    file = open("team_draft_pick_success.csv", 'w')
    file.write("TeamName,TotalPicks,ReachedMajors\n")
    for team_id in range(1, 31):
        query = """
                    SELECT t3.team_name, COUNT(*)
                    FROM TEAMS as t3
                    JOIN TEAM_DRAFTED as t2 ON t3.id = t2.team_id
                    JOIN DRAFTED_BY_TEAM as t1 ON t2.team_id = t1.team_id
                    WHERE t3.id = ? AND t1.reached_majors = TRUE AND t2.valid = TRUE
                    GROUP BY t3.team_name
                """
        output = cur.execute(query, (team_id,)).fetchone()
        if output is None:
            print(f"Team ID: {team_id} is invalid")
        else:
            team_name, reached_majors = output
        query2 = """
                    SELECT t3.team_name, COUNT(*)
                    FROM TEAMS as t3
                    JOIN TEAM_DRAFTED as t2 ON t3.id = t2.team_id
                    JOIN DRAFTED_BY_TEAM as t1 ON t2.team_id = t1.team_id
                    WHERE t3.id = ? AND t2.valid = TRUE
                    GROUP BY t3.team_name
                """
        output2 = cur.execute(query2, (team_id,)).fetchone()
        if output2 is None:
            print(f"Team ID: {team_id} is invalid")
        else:
            team_name2, total_signed_picks = output2
        assert(team_name == team_name2)
        print(f"Team ID: {team_name}")
        print(f"Total Picks: {total_signed_picks}")
        print(f"Reach Majors: {reached_majors}")
        file.write(f"{team_name},{total_signed_picks},{reached_majors}\n")

    file.close()

def get_draft_year_success_rate(cur, conn):
    file = open("draft_year_success.csv", 'w')
    file.write("DraftYear,TotalPicks,ReachedMajors\n")
    for team_id in range(1990, 2016):
        query = """
                    SELECT t1.year, COUNT(*)
                    FROM DRAFTED_BY_TEAM as t1
                    WHERE t1.year = ? AND t1.reached_majors = TRUE
                    GROUP BY t1.year
                """
        output = cur.execute(query, (team_id,)).fetchone()
        year, reached_majors = output
        query2 = """
                    SELECT t1.year, COUNT(*)
                    FROM DRAFTED_BY_TEAM as t1
                    WHERE t1.year = ?
                    GROUP BY t1.year
                """
        output2 = cur.execute(query2,(team_id,)).fetchone()
        year2, total_signed_picks = output2
        assert(year == year2)
        print(f"Year: {year}")
        print(f"Total Picks: {total_signed_picks}")
        print(f"Reach Majors: {reached_majors}")
        file.write(f"{year},{total_signed_picks},{reached_majors}\n")

    file.close()

def create_draft_pick_success_plot():
    pick_nums = [str(i) for i in range(1,301)]
    success_rates = []
    with open("draft_pick_success.csv", 'r') as file:
        file.readline()
        for row in file:
            nums = row.split(",")
            rate = int(nums[2]) / int(nums[1])
            success_rates.append(rate)

    fig, ax = plt.subplots()
    ax.plot(pick_nums, success_rates)
    ax.set_xlabel('Overall Pick')
    ax.set_ylabel('Percent of picks that appeared in MLB')
    ax.set_title('Draft success rate by overall pick slot')
    ax.xaxis.set_ticks(range(19,301,20))
    plt.show()

def main():
    """
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

    get_number_draft_picks_reach_majors(cur, conn)
    get_team_success_rate(cur, conn)
    get_draft_year_success_rate(cur, conn)

    # create_draft_pick_success_plot()

    # Close database connection
    conn.close()

main()
