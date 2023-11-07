from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import re

chrome_options = Options()
chrome_options.add_argument("--headless")
driver = webdriver.Chrome()

url = "https://www.bovada.lv/sports/football"
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

# this puts the string into a dataframe. Sometimes one array is one item longer, drop first item if the case, don't need first anyways
date_pattern = r'\d{1,2}/\d{1,2}/\d{2}'
dates = re.findall(date_pattern, data_as_string)
teams = re.split(date_pattern, data_as_string)
teams = [team.strip() for team in teams if team.strip()]
if len(teams) == len(dates) + 1:
    teams = teams[1:]
df = pd.DataFrame({'date': dates, 'team': teams})
df2 = df.copy()

# date_pattern = r'\d{1,2}/\d{1,2}/\d{2}'
# dates = re.findall(date_pattern, ' '.join(df['team']))
# teams = re.split(date_pattern, ' '.join(df['team']))
# teams = [team.strip() for team in teams if team.strip()]
# df2 = pd.DataFrame({'date': dates, 'team': teams})

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

#final cleanup
df3['team'] = df3['team'].str.replace(r' \(.+\)', '', regex=True) #remove college team rankings in team name
df3['spread_payout'] = df3['spread_payout'].replace('EVEN', '+100') #change EVEN to +100
df3['moneyline'] = df3['moneyline'].replace('EVEN', '+100') #change EVEN to +100
df3['over_under_payout'] = df3['over_under_payout'].replace('EVEN', '+100') #change EVEN to +100