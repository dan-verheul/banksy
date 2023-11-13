from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
start_time = time.time()
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

while True:
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(options=chrome_options)

    url = "https://www.bovada.lv/sports/football/nfl"
    driver.get(url)
    wait = WebDriverWait(driver, 10)
    sp_main_area = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "sp-main-area")))
    div_contents = sp_main_area.get_attribute("innerHTML")
    soup = BeautifulSoup(div_contents, "html.parser")
    div_text = soup.get_text()
    split_pattern = r'\n' 
    lines = re.split(split_pattern, div_text)
    data_as_string = '\n'.join(lines)

    driver.quit()

    #sometimes the scrape doesn't scrape the right portion of the website, sometimes it does..so that's fun (praying for an API)
    #added this loop to check final output string to make sure it's pulling everything
    if len(data_as_string) < 250:
        elapsed_time = time.time() - start_time
        if elapsed_time >= 60:
            skip_bovada = "Skip Bovada - scrape took over 1 minute" #adding this so it doesn't run through my 100th bday
            break  # Quit the loop
        else:
            time.sleep(2)
    else:
        break

if 'skip_bovada' not in locals() and 'skip_bovada' not in globals(): #run code only if we have a string to work with
    # this puts the string into a dataframe. Sometimes one array is one item longer, drop first item if the case, don't need first anyways
    date_pattern = r'\d{1,2}/\d{1,2}/\d{2}'
    dates = re.findall(date_pattern, data_as_string)
    teams = re.split(date_pattern, data_as_string)
    teams = [team.strip() for team in teams if team.strip()]
    if len(teams) == len(dates) + 1:
        teams = teams[1:]
    df = pd.DataFrame({'date': dates, 'team': teams})
    df2 = df.copy()

    #if column 2 ends eith " Bets" and less than 25 characters then delete row
    df2 = df[~(df['team'].str.len() < 25) | ~(df['team'].str.endswith(' Bets'))]
    #get starting point of AM or PM, then add 3 for "AM " or "PM ", then remove left character from string that amount
    df2['am_pm'] = df2['team'].apply(lambda x: x.find('AM') if 'AM' in x else x.find('PM')) + 3 
    df2['full_info'] = df2.apply(lambda row: row['team'][row['am_pm']:] if row['am_pm'] >= 0 else row['team'], axis=1)
    df2 = df2.drop(columns=['am_pm','team'])

    #get teams
    df2['first_symbol'] = df2['full_info'].apply(lambda x: x.find(' + ') if ' + ' in x else x.find(' - '))
    df2['teams'] = df2.apply(lambda row: row['full_info'][:row['first_symbol']] if row['first_symbol'] >= 0 else row['full_info'], axis=1)
    df2['full_info'] = df2.apply(lambda row: row['full_info'][row['first_symbol']:] if row['first_symbol'] >= 0 else row['full_info'], axis=1).str[3:] #the end removes either the " + " or " - "
    df2 = df2.drop(columns=['first_symbol'])

    #get spread1 and payout1, then remove the info from full info
    spread1_pattern = r'([-+].*?)\(' #regex to pull from first "-" or "+" through the first (
    df2['spread_1'] = df2['full_info'].str.extract(spread1_pattern)
    payout1_pattern = r'\((.*?)\)' #regex to pull characters after the first "(" until the first ")"
    df2['spread_payout_1'] = df2['full_info'].str.extract(payout1_pattern)
    df2['full_info'] = df2['full_info'].str.replace(r'^.*?\)', '', regex=True)

    #same thing for team2
    spread2_pattern = r'([-+].*?)\(' #regex to pull from first "-" or "+" through the first (
    df2['spread_2'] = df2['full_info'].str.extract(spread2_pattern)
    payout2_pattern = r'\((.*?)\)' #regex to pull characters after the first "(" until the first ")"
    df2['spread_payout_2'] = df2['full_info'].str.extract(payout2_pattern)
    df2['full_info'] = df2['full_info'].str.replace(r'^.*?\)', '', regex=True)

    #moneyline for team 1 and team 2, then remove info from full_info
    df2['moneyline_1'] = df2['full_info'].str.split().str[0]
    df2['moneyline_2'] = df2['full_info'].str.split().str[1]
    df2['full_info'] = df2['full_info'].str.extract(r'([OU].*)')

    #get over/under for team1
    pattern_over_under = r'(\S+)'
    df2['over_under_1'] = df2['full_info'].str.extract(pattern_over_under)
    pattern_payout = r'\((.*?)\)'
    df2['over_under_payout_1'] = df2['full_info'].str.extract(pattern_payout)
    df2['full_info'] = df2['full_info'].str.replace(r'^.*?\)', '', regex=True) #remove team 1 info from full_info column
    #do the same thing for team2
    pattern_over_under = r'(\S+)'
    df2['over_under_2'] = df2['full_info'].str.extract(pattern_over_under)
    pattern_payout = r'\((.*?)\)'
    df2['over_under_payout_2'] = df2['full_info'].str.extract(pattern_payout)

    #drop full_info column
    df2 = df2.drop(columns=['full_info'])


    #fix teams column, separate into team_1 and team_2
    #regex to find the first capital letter without a space before it
    def find_team_1(text):
        for i, char in enumerate(text[1:], 1):  # Start from the second character
            if char.isupper():
                if i == 1 or text[i - 1] != ' ':
                    return text[:i]
        return text
    def find_team_2(text):
        for i, char in enumerate(text[1:], 1):  # Start from the second character
            if char.isupper():
                if i == 1 or text[i - 1] != ' ':
                    return text[i:]
        return text
    df2['team_1'] = df2['teams'].apply(find_team_1)
    df2['team_2'] = df2['teams'].apply(find_team_2)

    #now i want to reorder cols and remove teams column
    df2 = df2[['date', 'team_1', 'team_2', 'spread_1', 'spread_payout_1', 'spread_2', 'spread_payout_2', 'moneyline_1', 'moneyline_2', 'over_under_1', 'over_under_payout_1','over_under_2', 'over_under_payout_2']]
    #duplicate rows
    df3 = pd.concat([df2] * 2, ignore_index=True)
    df3 = df3.sort_values(by=['date', 'team_1'])
    df3.reset_index(drop=True, inplace=True)
    #remove data from every other row for columns ending in _1
    for index, row in df3.iterrows():
        if index % 2 == 0:
            for column in df3.columns:
                if column.endswith("_1"):
                    df3.at[index, column] = ''
    for index, row in df3.iterrows():
        if index % 2 == 1:
            for column in df3.columns:
                if column.endswith("_2"):
                    df3.at[index, column] = ''

    #now we want to collapse all of the columns onto eachother 
    df3['team'] = df3['team_1'].mask(df3['team_1'] == '', df3['team_2'])
    df3['spread'] = df3['spread_1'].mask(df3['spread_1'] == '', df3['spread_2'])
    df3['spread_payout'] = df3['spread_payout_1'].mask(df3['spread_payout_1'] == '', df3['spread_payout_2'])
    df3['moneyline'] = df3['moneyline_1'].mask(df3['moneyline_1'] == '', df3['moneyline_2'])
    df3['over_under'] = df3['over_under_1'].mask(df3['over_under_1'] == '', df3['over_under_2'])
    df3['over_under_payout'] = df3['over_under_payout_1'].mask(df3['over_under_payout_1'] == '', df3['over_under_payout_2'])

    #drop columns
    columns_to_drop = [col for col in df3.columns if col.endswith(('_1', '_2'))]
    df3 = df3.drop(columns=columns_to_drop)

    #cleanup
    df3['team'] = df3['team'].str.replace(r' \(.+\)', '', regex=True) #remove college team rankings in team name
    df3['spread_payout'] = df3['spread_payout'].replace('EVEN', '+100') #change EVEN to +100
    df3['moneyline'] = df3['moneyline'].replace('EVEN', '+100') #change EVEN to +100
    df3['over_under_payout'] = df3['over_under_payout'].replace('EVEN', '+100') #change EVEN to +100

    #delete the second time teams play. Ex: if Bills play MNF and you pull monday morning it will show Bills game for tonight and this weekend, remove this weekends game (and opponent too)
    #too much of a nightmare trying to keep both instances (can add in later)
    game_id_values = [i // 2 + 1 for i in range(len(df3))]
    df3['game_id'] = game_id_values
    df3['Rank'] = df3.groupby('team').cumcount() + 1
    game_ids_to_remove = df3[df3['Rank'] == 2]['game_id'].unique()
    df3 = df3[~df3['game_id'].isin(game_ids_to_remove)]

    # match structure from final_df in arb_scanner
    #1: create ML df
    bovada_ml_df = df3[['team', 'moneyline']]
    bovada_ml_df['team'] = bovada_ml_df['team'].str.split().str[-1] #only keep team name (Seahawks) instead of Seattle Seahawks to help with join to final_df
    bovada_ml_df.rename(columns={'team': 'Team', 'moneyline': 'Bovada'}, inplace=True)
    bovada_ml_df['Bet Type'] = 'ML'
    bovada_ml_df['Info'] = 'Payout'
    #sometimes there are blank values in Bovada, when this happens another col gets duplicated. If value has "(", ")", "O", or "U" then replace value with ''
    bovada_ml_df['Bovada'] = bovada_ml_df['Bovada'].apply(lambda x: '' if any(char in x for char in '()OU') else x)

    #2: create spread df
    bovada_spread_df = df3[['team', 'spread','spread_payout']]
    bovada_spread_df = pd.concat([bovada_spread_df, bovada_spread_df], ignore_index=True) #duplicate each row
    bovada_spread_df = bovada_spread_df.sort_values(by='team').reset_index(drop=True)
    bovada_spread_df['Info'] = ['Line', 'Payout'] * (len(bovada_spread_df) //2)  #duplicates rows and creates Info column
    bovada_spread_df['Bovada'] = bovada_spread_df.apply(lambda row: row['spread'] if row['Info'] == 'Line' else row['spread_payout'], axis=1) #collapse cols into one Bovada col
    bovada_spread_df['team'] = bovada_spread_df['team'].str.split().str[-1] #only keep team name (Seahawks) instead of Seattle Seahawks to help with join to final_df
    bovada_spread_df = bovada_spread_df.drop(columns=['spread','spread_payout']) #drop old cols
    bovada_spread_df.rename(columns={'team': 'Team'}, inplace=True) #rename to help with join
    bovada_spread_df['Bet Type'] = 'Spread'
   
    #3: create overunder df
    bovada_ou_df = df3[['team', 'over_under','over_under_payout']]
    bovada_ou_df = pd.concat([bovada_ou_df, bovada_ou_df], ignore_index=True) #duplicate each row
    bovada_ou_df = bovada_ou_df.sort_values(by='team').reset_index(drop=True)
    bovada_ou_df['Info'] = ['Line', 'Payout'] * (len(bovada_ou_df) //2)  #duplicates rows and creates Info column
    bovada_ou_df['Bovada'] = bovada_ou_df.apply(lambda row: row['over_under'] if row['Info'] == 'Line' else row['over_under_payout'], axis=1) #collapse cols into one Bovada col
    bovada_ou_df['team'] = bovada_ou_df['team'].str.split().str[-1] #only keep team name (Seahawks) instead of Seattle Seahawks to help with join to final_df
    bovada_ou_df = bovada_ou_df.drop(columns=['over_under','over_under_payout']) #drop old cols
    bovada_ou_df.rename(columns={'team': 'Team'}, inplace=True) #rename to help with join
    bovada_ou_df['Bovada'] = bovada_ou_df['Bovada'].str.replace('U', '-').str.replace('O', '+')
    bovada_ou_df['Bet Type'] = 'Over/Under'

    #4: Combine all the bovada dfs
    final_bovada_df = pd.concat([bovada_ml_df, bovada_spread_df, bovada_ou_df], ignore_index=True)
    final_bovada_df = final_bovada_df[['Team','Bet Type','Info','Bovada']]
    final_bovada_df = final_bovada_df.astype(str)
else:
    final_bovada_df = skip_bovada


#now we're gonna left join this to final_df on team, bet type, and info columns to just pull in Bovada col