from bs4 import BeautifulSoup as bs
import requests as req
import csv
import os
from time import sleep
from datetime import datetime as dt
from concurrent.futures import ThreadPoolExecutor, wait
import functools

#global config
user_agent = {'User-agent': "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"}
team_rel_abbr = {'New Orleans/Oklahoma City Hornets':'NOK','New Jersey Nets':'NJN','Vancouver Grizzlies':'VAN','Seattle SuperSonics':'SEA', 'Los Angeles Clippers':'LAC', 'New Orleans Pelicans':'NOP', 'Charlotte Hornets':'CHO', 'Cleveland Cavaliers': 'CLE', 'Miami Heat': 'MIA', 'Phoenix Suns': 'PHO', 'Dallas Mavericks': 'DAL', 'Detroit Pistons': 'DET', 'Charlotte Bobcats': 'CHA', 'Minnesota Timberwolves': 'MIN', 'New Orleans Hornets': 'NOH', 'Indiana Pacers': 'IND', 'Chicago Bulls': 'CHI', 'Oklahoma City Thunder': 'OKC', 'Los Angeles Lakers': 'LAL', 'Utah Jazz': 'UTA', 'Memphis Grizzlies': 'MEM', 'Portland Trail Blazers': 'POR', 'LA Clippers': 'LAC', 'Atlanta Hawks': 'ATL', 'Sacramento Kings': 'SAC', 'Philadelphia 76ers': 'PHI', 'Brooklyn Nets': 'BRK', 'Houston Rockets': 'HOU', 'Milwaukee Bucks': 'MIL', 'Boston Celtics': 'BOS', 'Denver Nuggets': 'DEN', 'Orlando Magic': 'ORL', 'Golden State Warriors': 'GSW', 'New York Knicks': 'NYK', 'Toronto Raptors': 'TOR', 'San Antonio Spurs': 'SAS', 'Washington Wizards': 'WAS'}
team_rel_abbr_old = {'New Orleans/Oklahoma City Hornets':'NOK','New Jersey Nets':'NJN','Vancouver Grizzlies':'VAN','Seattle SuperSonics':'SEA', 'Los Angeles Clippers':'LAC', 'New Orleans Pelicans':'NOP', 'Charlotte Hornets':'CHH', 'Cleveland Cavaliers': 'CLE', 'Miami Heat': 'MIA', 'Phoenix Suns': 'PHO', 'Dallas Mavericks': 'DAL', 'Detroit Pistons': 'DET', 'Charlotte Bobcats': 'CHA', 'Minnesota Timberwolves': 'MIN', 'New Orleans Hornets': 'NOH', 'Indiana Pacers': 'IND', 'Chicago Bulls': 'CHI', 'Oklahoma City Thunder': 'OKC', 'Los Angeles Lakers': 'LAL', 'Utah Jazz': 'UTA', 'Memphis Grizzlies': 'MEM', 'Portland Trail Blazers': 'POR', 'LA Clippers': 'LAC', 'Atlanta Hawks': 'ATL', 'Sacramento Kings': 'SAC', 'Philadelphia 76ers': 'PHI', 'Brooklyn Nets': 'BRK', 'Houston Rockets': 'HOU', 'Milwaukee Bucks': 'MIL', 'Boston Celtics': 'BOS', 'Denver Nuggets': 'DEN', 'Orlando Magic': 'ORL', 'Golden State Warriors': 'GSW', 'New York Knicks': 'NYK', 'Toronto Raptors': 'TOR', 'San Antonio Spurs': 'SAS', 'Washington Wizards': 'WAS'}

