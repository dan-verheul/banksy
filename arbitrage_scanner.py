#web scrape
import requests
from bs4 import BeautifulSoup

#google sheets
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

#push notifications
import http.client, urllib

#set working directory and pull in hidden variables
import os
current_directory = os.getcwd()
if os.path.basename(current_directory) != 'GitHub':
    parent_directory = os.path.abspath(os.path.join(current_directory, os.pardir)) #take one step back from current directory
    os.chdir(parent_directory)
from banksy_private.config import *

#general
import pandas as pd
import numpy as np
import pytz
import re
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

#constants
sport_list = ['MLB', 'NFL', 'NBA']
bookies_list = ['DraftKings', 'BetMGM', 'Caesars', 'FanDuel', 'RiversCasino', 'Bet365', 'PointsBet', 'Unibet', 'Consensus']
non_az_or_ny_bookies = ['Bet365', 'Consensus']
non_az_bookies = ['PointsBet']

############################################################################
############## Webscrape into dataframe
############################################################################
# Create DF based off of what is on this page, this pulls Spread, O/U, and Moneyline
dfs = []
for sport in sport_list:
    url = f"https://www.{website_1}/{sport}/{website_1_suffix}/"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table')
    data = []
    for row in table.find_all('tr'):
        cols = row.find_all(['th', 'td'])
        cols = [col.text.strip() for col in cols]
        data.append(cols)
    sport_df = pd.DataFrame(data)
    sport_df['Sport'] = sport
    dfs.append(sport_df)
original_df = pd.concat(dfs, ignore_index=True)

#Clean up df: remove blank rows, move title row to title of df, remove apps that we don't want
original_df = original_df[original_df.iloc[:, 0] != '']
original_df = original_df[original_df.iloc[:, 1].notna()]
original_df.columns = original_df.iloc[0]
original_df = original_df[1:]
original_df.reset_index(drop=True, inplace=True)
for sport in sport_list:
    if sport in original_df.columns:
        original_df.rename(columns={sport: 'Sport'}, inplace=True)


columns_to_keep = ['Time', 'Sport'] + bookies_list # 'Open', #This is Vegas Insider lines (I think)
columns_to_remove = [col for col in original_df.columns if col not in columns_to_keep]
setup_df = original_df.drop(columns=columns_to_remove)

# Continue cleanup: rename columns, clean columns(remove " +" from the end of value), etc
setup_df = setup_df.rename(columns={"Time": "Team"})
setup_df['Team'] = setup_df['Team'].str.replace(r'^\d+ ', '', regex=True)
def extract_second_word(text):
    parts = text.split()
    if len(parts) >= 3:
        return parts[0]
    else:
        return text
setup_df['Team'] = setup_df['Team'].apply(extract_second_word)

for column in bookies_list:
    setup_df[column] = setup_df[column].str.rstrip(" +")

#Create column that determines the bet type
def determine_bet_type(row):
    if any(op in row['Bet365'] for op in ('o', 'u')):
        return 'Over/Under'
    elif len(row['Bet365']) == 4:
        return 'ML'
    elif any(op in row['Bet365'] for op in ('+', '-')):
        return 'Spread'
    else:
        return 'ERROR'  
setup_df['Bet Type'] = setup_df.apply(determine_bet_type, axis=1)
setup_df = setup_df[['Bet Type'] + [col for col in setup_df.columns if col != 'Bet Type']]

# Separate into 3 dataframes, 1 for each bet type. Then apply game_id. Then combine them back together
df_spread = setup_df[setup_df['Bet Type'] == 'Spread'].copy()
game_id_values = [i // 2 + 1 for i in range(len(df_spread))]
df_spread['game_id'] = game_id_values

df_ml = setup_df[setup_df['Bet Type'] == 'ML'].copy()
game_id_values = [i // 2 + 1 for i in range(len(df_ml))]
df_ml['game_id'] = game_id_values

df_overunder = setup_df[setup_df['Bet Type'] == 'Over/Under'].copy()
game_id_values = [i // 2 + 1 for i in range(len(df_overunder))]
df_overunder['game_id'] = game_id_values

setup_df = pd.concat([df_spread, df_ml, df_overunder])
setup_df = setup_df.sort_index(ascending=True)


# Duplicate the rows and add a new column called "Info" that has either the Betting Line or the Payout information
duplicated_rows = []
info_pattern = ['Line', 'Payout']
for index, row in setup_df.iterrows():
    for i in range(2): 
        duplicated_row = list(row)
        duplicated_row.append(info_pattern[i])
        duplicated_rows.append(duplicated_row)
columns = list(setup_df.columns) + ['Info']
duplicated_df = pd.DataFrame(duplicated_rows, columns=columns)
# Moneyline shouldnt be seperated out into separate rows, so delete the rows where Bet Type = ML and Info = "Line"
duplicated_df = duplicated_df[(duplicated_df['Bet Type'] != 'ML') | (duplicated_df['Info'] != 'Line')]
duplicated_df = duplicated_df.reset_index(drop=True)

#Rename and reorder the columns
duplicated_df = duplicated_df.rename(columns={'game_id': 'Game ID'})
columns = ['Sport', 'Game ID', 'Team','Bet Type', 'Info'] + bookies_list
duplicated_df = duplicated_df[columns]

# we want to delete rows that have col titles in middle of the df, we hit this when we concat the df's
# delete rows where value under 'FanDuel' column is 'Fanduel'
for bookie in bookies_list:
    duplicated_df = duplicated_df[duplicated_df[bookie] != bookie]

# Create separate dataframes for the Betting Lines and the Payout
df_line = duplicated_df[duplicated_df['Info'] == 'Line'].copy()
df_payout = duplicated_df[duplicated_df['Info'] == 'Payout'].copy()

# Delete the payouts from the Lines dataframe
for column in bookies_list:
    df_line[column] = df_line[column].str.split(' ').str[0]

# Delete the lines from the payout dataframe
df_payout = duplicated_df[duplicated_df['Info'] == 'Payout'].copy()
def extract_right_of_space(value):
    if isinstance(value, str):
        space_position = value.find(' ')
        if space_position != -1:
            return value[space_position + 1:]
    return value
for bookie in bookies_list:
    df_payout[bookie] = df_payout[bookie].apply(extract_right_of_space)
#combine the dataframes back together
final_df = pd.concat([df_line, df_payout])
final_df = final_df.sort_index(ascending=True)
# sort final_df and push into google sheets
custom_sort_order = ['ML', 'Spread', 'Over/Under']
final_df['Bet Type'] = pd.Categorical(final_df['Bet Type'], categories=custom_sort_order, ordered=True)
final_df = final_df.sort_values(by=['Bet Type', 'Game ID', 'Team', 'Info'])


# cast as string and trim spaces
columns_to_strip = ['Team'] + bookies_list
for column in columns_to_strip:
    final_df[column] = final_df[column].astype(str).str.strip()

# find and replace 'even' odds with '+100', 'o' with '' and 'u' with '-' for over/under
for column in bookies_list:
    final_df[column] = final_df[column].replace({'even': '+100', 'o': '', 'u': '-'}, regex=True)

final_df = final_df.reset_index(drop=True)

# this loops through each of the csv's to pull the sport and team data
sports_lowercase = [sport.lower() for sport in sport_list]
merged_df = final_df.copy()
sport_data = {}
for sport in sports_lowercase:
    csv_filename = f"{sport}_teams.csv"
    file_path = os.path.join(os.getcwd(), "banksy", csv_filename)
    team_df = pd.read_csv(file_path)
    team_df = team_df[['Team', 'Sport', 'Abbreviation']]  # Select relevant columns
    team_df.rename(columns={'Abbreviation': f'Abbreviation_{sport}'}, inplace=True)
    merged_df = merged_df.merge(team_df, on=['Team', 'Sport'], how='left')  # Merge based on 'Team' and 'Sport'
    sport_data[sport] = {'Abbreviation': f'Abbreviation_{sport}', 'Sport': f'Sport_{sport}'} # Store sport data in the dictionary
# Loop through sports and update the 'Sport' and 'Abbreviation' columns
merged_df['Abbreviation'] = merged_df['Abbreviation_mlb'].fillna(merged_df['Abbreviation_nfl']).fillna(merged_df['Abbreviation_nba'])
merged_df.drop(['Abbreviation_mlb', 'Abbreviation_nfl', 'Abbreviation_nba'], axis=1, inplace=True)
final_df = merged_df

############################################################################
############## Final Scores DFs
############################################################################
##### Create a dataframe that has all finished games
# sport_list found at top of script
all_finished_games = []

for sport in sport_list:
    url = f"https://www.{website_2}/{sport.lower()}/{website_2_suffix}/"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table')
    data = []
    for row in table.find_all('tr'):
        cols = row.find_all(['th', 'td'])
        cols = [col.text.strip() for col in cols]
        data.append(cols)
    scores_df = pd.DataFrame(data)
    
    if not scores_df.empty:
        scores_df = scores_df.rename(columns={scores_df.columns[0]: 'Game Info'})
        scores_df = scores_df[scores_df['Game Info'].str.len() >= 50]
        scores_df = scores_df[scores_df['Game Info'].str.startswith('Final ')]
        scores_df = scores_df.iloc[:, [4, 7]]
        scores_df = scores_df.rename(columns={scores_df.columns[0]: 'Team1', scores_df.columns[1]: 'Team2'})
        if not scores_df.empty:
            scores_df['Team1'] = scores_df.apply(lambda row: "49ers" if row['Team1'][:5] == "49ers" else (re.match(r'^([A-Za-z]+).*', row['Team1']).group(1) if re.match(r'^([A-Za-z]+).*', row['Team1']) else None), axis=1)#this pulls all letters from value until the last letter
            scores_df['Team2'] = scores_df.apply(lambda row: "49ers" if row['Team2'][:5] == "49ers" else (re.match(r'^([A-Za-z]+).*', row['Team2']).group(1) if re.match(r'^([A-Za-z]+).*', row['Team2']) else None), axis=1)#this pulls all letters from value until the last letter
        finished_games = pd.DataFrame({'Team': pd.concat([scores_df['Team1'], scores_df['Team2']], axis=0, ignore_index=True)})
        finished_games['Sport'] = sport

        all_finished_games.append(finished_games)

if all_finished_games:
    game_over_df = pd.concat(all_finished_games, ignore_index=True)
else:
    game_over_df = pd.DataFrame()
game_over_df['Game Status'] = 'Game Over'

# pull in final games, then filter out any rows that have Game Over status
final_df = pd.merge(final_df, game_over_df, on=['Team', 'Sport'], how='left')
final_df = final_df.loc[final_df['Game Status'] != 'Game Over']

# on 11/1 kings warriors ML were N/A under Caesars, verified website said the same. Change N/As to the min value found on row
final_df.replace('N/A', None, inplace=True)

############################################################################
############## TESTING
############################################################################
# final_df.loc[1, 'PointsBet'] = 130
# final_df.loc[262, 'Bet365'] = 140
# final_df.at[259, 'BetMGM'] = 1110
# final_df.at[41, 'Caesars'] = 115


############################################################################
############## Betting logic
############################################################################
#cast columns as float
cols_to_float = ['Game ID'] + bookies_list
for column in cols_to_float:
    final_df[column] = pd.to_numeric(final_df[column], errors='coerce')

#payout df used for calcs
payout_df = final_df[final_df['Info'] == 'Payout'].copy()
#select max payout across the 4 columns
payout_df['MaxPayout'] = payout_df[bookies_list].max(axis=1)

#arbitrage calc
result = payout_df.groupby(['Game ID', 'Bet Type'])['MaxPayout'].sum().reset_index()
payout_df = payout_df.merge(result, on=['Game ID', 'Bet Type'], how='left', suffixes=('', '_Sum'))
payout_df = payout_df.rename(columns={'MaxPayout_Sum': 'Arbitrage Calc'})
#only show calc on payout lines, to help declutter
final_df = final_df.merge(payout_df[['Game ID', 'Team', 'Bet Type', 'Info', 'MaxPayout', 'Arbitrage Calc']],
                        on=['Game ID', 'Team', 'Bet Type', 'Info'],
                        how='left',
                        suffixes=('', '_Test'))
final_df['MaxPayout'] = final_df['MaxPayout'].fillna('')
final_df['Arbitrage Calc'] = final_df['Arbitrage Calc'].fillna('')

# Replace NaN values with empty strings in bookies_list columns and 'MaxPayout' and Arb Calc
cols_to_replace_nan = bookies_list + ['MaxPayout'] + ['Arbitrage Calc']
final_df[cols_to_replace_nan] = final_df[cols_to_replace_nan].fillna('')

############################################################################
############## Upload final_df into google sheets
############################################################################
#transform df
final_df['updated_at'] = pd.to_datetime(datetime.now())
final_df['updated_at'] = final_df['updated_at'].dt.strftime('%Y-%m-%d %H:%M')
google_sheets_df = final_df.copy()
drop_cols = ['Game Status', 'Abbreviation']
google_sheets_df.drop(drop_cols, axis=1, inplace=True)
replace = {None: 0}
google_sheets_df = google_sheets_df.replace(replace)

creds_file = google_sheets_json_file
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scope)
client = gspread.authorize(creds)

sheet_title = 'Python'
spreadsheet = client.open(sheet_title)
worksheet = spreadsheet.get_worksheet(1)
worksheet.clear()
start_cell = 'A1'
data = google_sheets_df.values.tolist()
data = [google_sheets_df.columns.tolist()] + google_sheets_df.values.tolist()
worksheet.update(start_cell, data)


############################################################################
############## ALERT ALERT ALERT
############################################################################
for column in bookies_list:
    final_df[column] = final_df[column].replace('', np.nan).astype(float)
final_df['MaxPayout'] = pd.to_numeric(final_df['MaxPayout'], errors='coerce')
final_df['Arbitrage Calc'] = pd.to_numeric(final_df['Arbitrage Calc'], errors='coerce')
alert_df = pd.DataFrame()
alert_df = final_df[final_df['Arbitrage Calc'] > 0]
alert_df = alert_df[~((alert_df['MaxPayout'] == 100) & (alert_df['Arbitrage Calc'] == 200))]

#restructure final_df
betting_dfs = []
for bookie in bookies_list:
    columns_to_select = ['Game ID', 'Team', 'Bet Type', 'Info', bookie]
    bookie_df = final_df[columns_to_select].copy()
    bookie_df['Bookie'] = bookie
    bookie_df = bookie_df.rename(columns={bookie: "Value"})
    bookie_df = bookie_df[['Bookie', 'Game ID', 'Team', 'Bet Type', 'Info', 'Value']]
    betting_dfs.append(bookie_df)
mega_df = pd.concat(betting_dfs, ignore_index=True)

if len(alert_df) > 0:    
    alert_df['Team'] = alert_df['Team'].str.strip()
    alert_df['Bet Type'] = alert_df['Bet Type'].str.strip()

    #get bookie column
    def find_max_payout_column(row):
        for col in alert_df.columns:
            if row[col] == row['MaxPayout']:
                return col
        return None
    alert_df['Bookie'] = alert_df.apply(find_max_payout_column, axis=1)
    #merge df's together
    alert_df = alert_df.merge(mega_df, on=['Bookie', 'Game ID', 'Team', 'Bet Type', 'Info'])

    #calculate profit margin on $100 stake
    def custom_formula(value):
        if value > 0:
            return (value / 100) + 1
        elif value < 0:
            return (100 / abs(value)) + 1
        else:
            return 1
    alert_df['decimal'] = alert_df['Value'].apply(custom_formula)
    alert_df['payout'] = alert_df.apply(lambda row: row['decimal'] * 100 if row.name == 0 or row.name % 2 == 0 else np.nan, axis=1)
    alert_df['payout'] = alert_df['payout'].fillna(method='ffill')
    alert_df['stake'] = alert_df.apply(lambda row: 100 if row.name == 0 or row.name % 2 == 0 else row['payout']/row['decimal'], axis=1).round(2)
    
    #get total stake amount per bet
    grouped = alert_df.groupby(['Game ID', 'Bet Type'])['stake'].sum().reset_index()
    alert_df = pd.merge(alert_df, grouped, on=['Game ID', 'Bet Type'], suffixes=('', '_sum'))
    alert_df = alert_df.rename(columns={'stake_sum': 'total_stake'})
    
    #profit margin calculation
    alert_df['Profit Margin'] = ((alert_df['payout'] - alert_df['total_stake']) / alert_df['total_stake'] * 100).round(0).astype(int)

    #add + to value column
    def custom_value(row):
        if row['Value'] > 0:
            return '+' + str(row['Value'])
        else:
            return str(row['Value'])
    alert_df['Final Bet Line'] = alert_df.apply(custom_value, axis=1)

    #add + to value column
    def final_bet_type(row):
        return row['Bet Type']
    alert_df['Final Bet Type'] = alert_df.apply(final_bet_type, axis=1)
    
    # order df by profit margin decreasing, game id
    alert_df = alert_df.sort_values(by=['Profit Margin', 'Game ID'], ascending=[False, True])

    #re order game id column for final output, so we start at 1
    game_id_values = [i // 2 + 1 for i in range(len(alert_df))]
    alert_df['Game ID'] = game_id_values

    #set threshold to filter out profit margins below x%
    final_alert_df = alert_df[alert_df['Profit Margin'] >= 3]

    #get the line if the notification is spread or over/underbet type
    #pull lines, transpose to new df, this will be joined to alertdf
    final_df_line = final_df[final_df['Info'] == 'Line']
    final_df_line.drop('MaxPayout', axis=1, inplace=True)
    final_df_line.drop('Arbitrage Calc', axis=1, inplace=True)
    final_df_line.drop('updated_at', axis=1, inplace=True)
    final_df_line['Line'] = None
    final_df_line = pd.melt(final_df_line, id_vars=['Sport', 'Game ID', 'Team', 'Abbreviation', 'Bet Type', 'Info'], var_name='Bookie', value_name='LineValues')
    
    # add + if line is positive
    def add_plus_if_positive(value):
        if value is not None and float(value) > 0:
            return "+" + str(value)
        else:
            return str(value)
    final_df_line['LineValues'] = final_df_line['LineValues'].apply(add_plus_if_positive)

    #left join line df to alert_df
    final_alert_df = pd.merge(final_alert_df, final_df_line, on=['Team', 'Bet Type','Bookie'], how='left')
    columns_to_drop = [col for col in final_alert_df.columns if col.endswith('_y')]
    final_alert_df.drop(columns=columns_to_drop, inplace=True)
    final_alert_df = final_alert_df.rename(columns={'Game ID_x': 'Game ID', 'Info_x':'Info', 'Sport_x': 'Sport', 'Abbreviation_x': 'Abbreviation'})

    #check to make sure that if the bet type is spread that you do not bet on two +'s or -'s. Ex: make sure both bets are not -120 or +150 or whatever
    sign_audit_df = final_alert_df.copy() 
    sign_audit_df['Sign'] = sign_audit_df['LineValues'].str[0]
    sign_audit_filter = sign_audit_df.groupby(['Game ID', 'Sport'])['Sign'].transform('nunique').ne(1)
    # this overwrites final_alert_df with only the true values from the sign_audit_filter
    final_alert_df = sign_audit_df[sign_audit_filter].reset_index(drop=True)

    #send push notification
    if len(final_alert_df) > 1:
        # read from google sheets
        #   if update not found on update page then send update
        #   if update found 2 times or less with same date then send update
        #   if more than 3 notifcations already recorded, don't send update
        spreadsheet = client.open('Python')
        worksheet = spreadsheet.get_worksheet(2)  # Change the index as needed
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)
        if len(df) >= 1:
            notifications_sent = df[['Team','updated_at']]
            notifications_sent['updated_at'] = pd.to_datetime(notifications_sent['updated_at'])
            team_daily_notification_count = notifications_sent.groupby(['updated_at', 'Team']).size().reset_index(name='Count')
        else:
            columns = ['Teams', 'updated_at']
            team_daily_notification_count = pd.DataFrame(columns=columns)

        # left join team_daily_notification_count to df. if value is NaN, 1, 2, or 3 then send notification
        final_alert_df['updated_at'] = pd.to_datetime(final_alert_df['updated_at'])
        final_alert_df['updated_at'] = final_alert_df['updated_at'].dt.date
        team_daily_notification_count['updated_at'] = pd.to_datetime(team_daily_notification_count['updated_at'])
        team_daily_notification_count['updated_at'] = team_daily_notification_count['updated_at'].dt.date
        if len(team_daily_notification_count) > 0:   
            notification_df = final_alert_df.merge(team_daily_notification_count, on=['Team', 'updated_at'], how='left').drop(columns='updated_at')
            notification_df = notification_df[(notification_df['Count'] < 3) | (notification_df['Count'].isna())]
        else:
            notification_df = final_alert_df

        #re order game id column for final output, so we start at 1
        if len(notification_df) > 0:
            game_id_values = [i // 2 + 1 for i in range(len(notification_df))]
            notification_df['Game ID'] = game_id_values

            # remove combinations that require a bookie not allowed in either NY or AZ
            to_remove = notification_df[notification_df['Bookie'].isin(non_az_or_ny_bookies)][['Game ID', 'Sport']]
            notification_df = notification_df[~notification_df.set_index(['Game ID', 'Sport']).index.isin(to_remove.set_index(['Game ID', 'Sport']).index)]
            game_id_values = [i // 2 + 1 for i in range(len(notification_df))]
            notification_df['Game ID'] = game_id_values
            # put a star next to NY only bet
            to_update = notification_df[notification_df['Bookie'].isin(non_az_bookies)][['Game ID', 'Sport']]
            notification_df.loc[notification_df.set_index(['Game ID', 'Sport']).index.isin(to_update.set_index(['Game ID', 'Sport']).index), 'Sport'] = '*' + notification_df['Sport']

            if len(notification_df) > 0:
                notification_df['Combined'] = (
                    notification_df['Sport'].astype(str) + ' ' +
                    notification_df['Game ID'].astype(str) + ': ' +
                    np.where(notification_df['Final Bet Type'] == 'Spread',
                        notification_df['LineValues'].astype(str),
                        notification_df['Final Bet Type'].astype(str)
                        ) + ' ' +
                    notification_df['Abbreviation'].astype(str) + ' ' +
                    notification_df['Final Bet Line'].astype(str) + ', ' +
                    notification_df['Bookie'].astype(str) + ' +' +
                    notification_df['Profit Margin'].astype(str) + '%'
                )
                combined_values = notification_df['Combined'].astype(str)
                result_string = '\n'.join(combined_values)

                if len(result_string) > 0:
                    #send update
                    conn = http.client.HTTPSConnection("api.pushover.net:443")
                    conn.request("POST", "/1/messages.json",
                    urllib.parse.urlencode({
                        "token": user_token,
                        "user": user,
                        "message": result_string + '\nt.ly/5smor',
                    }), { "Content-type": "application/x-www-form-urlencoded" })
                    conn.getresponse()

                    #remove * from message, just to keep sheets clean
                    notification_df['Sport'] = notification_df['Sport'].str.lstrip('*')
                    # write update to google sheets
                    worksheet = spreadsheet.get_worksheet(2)
                    sheets_upload = notification_df[['Sport','Game ID','Bookie','Team','Bet Type','Final Bet Line','Profit Margin','Combined']]
                    current_time_seattle = datetime.now(pytz.timezone('America/Los_Angeles'))
                    sheets_upload['updated_at'] = current_time_seattle.strftime('%Y-%m-%d')
                    data_to_append = sheets_upload.astype(str).values.tolist()
                    existing_data = worksheet.col_values(1)
                    last_row = len(existing_data) + 1  # The next row after the last row with data
                    worksheet.insert_rows(data_to_append, last_row)
