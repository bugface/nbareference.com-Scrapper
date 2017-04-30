from bs4 import BeautifulSoup as bs
import requests as req
import csv
import os
from time import sleep
from datetime import datetime as dt
#from csv import Sniffer
#from openpyxl import load_workbook, Workbook

user_agent = {'User-agent': "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"}
team_rel_abbr = {'New Orleans/Oklahoma City Hornets':'NOK','New Jersey Nets':'NJN','Vancouver Grizzlies':'VAN','Seattle SuperSonics':'SEA', 'Los Angeles Clippers':'LAC', 'New Orleans Pelicans':'NOP', 'Charlotte Hornets':'CHO', 'Cleveland Cavaliers': 'CLE', 'Miami Heat': 'MIA', 'Phoenix Suns': 'PHO', 'Dallas Mavericks': 'DAL', 'Detroit Pistons': 'DET', 'Charlotte Bobcats': 'CHA', 'Minnesota Timberwolves': 'MIN', 'New Orleans Hornets': 'NOH', 'Indiana Pacers': 'IND', 'Chicago Bulls': 'CHI', 'Oklahoma City Thunder': 'OKC', 'Los Angeles Lakers': 'LAL', 'Utah Jazz': 'UTA', 'Memphis Grizzlies': 'MEM', 'Portland Trail Blazers': 'POR', 'LA Clippers': 'LAC', 'Atlanta Hawks': 'ATL', 'Sacramento Kings': 'SAC', 'Philadelphia 76ers': 'PHI', 'Brooklyn Nets': 'BRK', 'Houston Rockets': 'HOU', 'Milwaukee Bucks': 'MIL', 'Boston Celtics': 'BOS', 'Denver Nuggets': 'DEN', 'Orlando Magic': 'ORL', 'Golden State Warriors': 'GSW', 'New York Knicks': 'NYK', 'Toronto Raptors': 'TOR', 'San Antonio Spurs': 'SAS', 'Washington Wizards': 'WAS'}
team_rel_abbr_old = {'New Orleans/Oklahoma City Hornets':'NOK','New Jersey Nets':'NJN','Vancouver Grizzlies':'VAN','Seattle SuperSonics':'SEA', 'Los Angeles Clippers':'LAC', 'New Orleans Pelicans':'NOP', 'Charlotte Hornets':'CHH', 'Cleveland Cavaliers': 'CLE', 'Miami Heat': 'MIA', 'Phoenix Suns': 'PHO', 'Dallas Mavericks': 'DAL', 'Detroit Pistons': 'DET', 'Charlotte Bobcats': 'CHA', 'Minnesota Timberwolves': 'MIN', 'New Orleans Hornets': 'NOH', 'Indiana Pacers': 'IND', 'Chicago Bulls': 'CHI', 'Oklahoma City Thunder': 'OKC', 'Los Angeles Lakers': 'LAL', 'Utah Jazz': 'UTA', 'Memphis Grizzlies': 'MEM', 'Portland Trail Blazers': 'POR', 'LA Clippers': 'LAC', 'Atlanta Hawks': 'ATL', 'Sacramento Kings': 'SAC', 'Philadelphia 76ers': 'PHI', 'Brooklyn Nets': 'BRK', 'Houston Rockets': 'HOU', 'Milwaukee Bucks': 'MIL', 'Boston Celtics': 'BOS', 'Denver Nuggets': 'DEN', 'Orlando Magic': 'ORL', 'Golden State Warriors': 'GSW', 'New York Knicks': 'NYK', 'Toronto Raptors': 'TOR', 'San Antonio Spurs': 'SAS', 'Washington Wizards': 'WAS'}

def get_team_regular(season, month):
	sleep(1)
	url_team_score = "http://www.basketball-reference.com/leagues/NBA_{}_games-{}.html".format(season, month)
	print(url_team_score)

	game = []

	r_team = None
	r_date = None

	rep_team = req.get(url_team_score, headers=user_agent)
	soup_team = bs(rep_team.text, "html.parser")
	
	thead = soup_team.find("thead")
	ths = thead.find_all("th")
	tbody = soup_team.find("tbody")
	trs = tbody.find_all('tr')

	with open("team_data.csv", "a+", encoding='utf-8', newline='') as f_team:
		writer = csv.writer(f_team)
		#if not Sniffer().has_header(f_team.read()):
		l = []
		if os.stat("team_data.csv").st_size == 0:
			for each in ths:
				l.append(each.get_text())
			writer.writerow(l)
		for each_tr in trs:
			l = []
			for i, each_t in enumerate(each_tr.findChildren(['th', 'td'])):			
				if i == 4:
					r_team = team_rel_abbr[each_t.getText()]
				elif i == 2:
					a_team = team_rel_abbr[each_t.getText()]
				elif i == 0:
					temp = each_t.getText().split(",")
					if temp[0] == 'Playoffs':
						writer.writerow(l)
						return game
					temp1 = temp[1].strip().split(' ')
					#print(temp1)
					extra_date = "-".join([temp1[0].strip(), temp1[1].strip(), temp[2].strip()])
					date_obj = dt.strptime(extra_date, "%b-%d-%Y")
					r_date = dt.strftime(date_obj, "%Y%m%d")
				l.append(each_t.getText())
			game.append((r_date,r_team,a_team))		
			writer.writerow(l)
	return game

def get_box(date, team_abbr, away, file):
	sleep(1)
	url_box_score = "http://www.basketball-reference.com/boxscores/{}0{}.html".format(date, team_abbr)
	print(url_box_score)
	rep_box = req.get(url_box_score, headers=user_agent)
	soup_box = bs(rep_box.text, "html.parser")

	div_id_ht = "all_box_{}_basic".format(team_abbr.lower())
	div_id_at = "all_box_{}_basic".format(away.lower())

	#div for box of away team
	a_div = soup_box.find('div', id = div_id_at)
	a_thead = a_div.find("thead")
	a_ths = a_thead.find_all("th")
	a_tbody = a_div.find("tbody")
	a_trs = a_tbody.find_all("tr")

	with open(file, "a+", encoding='utf-8', newline='') as f_box:
		writer = csv.writer(f_box)
		l = []
		if os.stat(file).st_size == 0:
			for each in a_ths:
				if not (each.getText() == "" or each.getText() == "Basic Box Score Stats"):
					l.append(each.get_text())
			l.append('team')
			l.append('date')
			writer.writerow(l)
		for each in a_trs:
			l = []
			for i, content in enumerate(each.findChildren(['th', 'td'])):
				if content.getText() == '':
					l.append('0')
				else:
					l.append(content.getText())
			l.append(away)
			l.append(date)
			if l[0] == 'Reserves':
				continue
			if l[1] == 'Did Not Play' or l[1] == 'Not With Team' or l[1] == 'Did Not Dress' or l[1] == 'Player Suspended':
				continue
			else:
				writer.writerow(l)

	#div for box of hoome team
	h_div = soup_box.find("div", id=div_id_ht)
	#h_thead = h_div.find("thead")
	#h_ths = h_thead.find_all("th")
	h_tbody = h_div.find("tbody")
	h_trs = h_tbody.find_all("tr")

	with open(file, "a+", encoding='utf-8', newline='') as f_box:
		writer = csv.writer(f_box)
		#l = []
		# if os.stat("box_data.csv").st_size == 0:
		# 	for each in h_ths:
		# 		if not (each.getText() == "" or each.getText() == "Basic Box Score Stats"):
		# 			l.append(each.get_text())
		# 	l.append('team')
		# 	writer.writerow(l)
		for each in h_trs:
			l = []
			for i, content in enumerate(each.findChildren(['th', 'td'])):
				if content.getText() == '':
					l.append('0')
				else:
					l.append(content.getText())
			l.append(team_abbr)
			l.append(date)
			if l[0] == 'Reserves':
				continue
			if l[1] == 'Did Not Play':
				continue
			else:
				writer.writerow(l)

def test():
	# get_box('20160413', 'GSW', 'MEM')
	season = 2007
	months = ['october', 'november', 'december', 'january', 'february', 'march', 'april']
	for month in months:
		# if month == 'october':
		# 	continue
		# else:
		game = get_team_regular(season, month)
		for each_game in game:
			get_box(each_game[0], each_game[1], each_game[2])
	
def regular():
	#regular season
	months = ['october', 'november', 'december', 'january', 'february', 'march', 'april']
	seasons = [x for x in range(2010, 2018)]
	for season in seasons:
		for month in months:
			# try:
			if season == 2012 and (month == 'october' or month == 'november'):
				continue
			elif season == 2005 and month == 'october':
				continue
			else:
				game = get_team_regular(season, month)
				#print(game[2][2])
				for each_game in game:
					get_box(each_game[0], each_game[1], each_game[2], "box_data.csv")
			# except:
			# 	continue
			# 	print('error')

def get_team_playoff(season):
	sleep(1)
	url_team_score = "http://www.basketball-reference.com/playoffs/NBA_{}_games.html".format(season)
	print(url_team_score)

	if season < 2010:
		team_r_abbr = team_rel_abbr_old
	else:
		team_r_abbr = team_rel_abbr

	game = []

	r_team = None
	r_date = None

	rep_team = req.get(url_team_score, headers=user_agent)
	soup_team = bs(rep_team.text, "html.parser")
	
	thead = soup_team.find("thead")
	ths = thead.find_all("th")
	tbody = soup_team.find("tbody")
	trs = tbody.find_all('tr')

	with open("team_data_playoff.csv", "a+", encoding='utf-8', newline='') as f_team:
		writer = csv.writer(f_team)
		#if not Sniffer().has_header(f_team.read()):
		l = []
		if os.stat("team_data_playoff.csv").st_size == 0:
			for each in ths:
				l.append(each.get_text())
			writer.writerow(l)
		for each_tr in trs:
			l = []
			for i, each_t in enumerate(each_tr.findChildren(['th', 'td'])):			
				if i == 4:
					r_team = team_r_abbr[each_t.getText()]
				elif i == 2:
					a_team = team_r_abbr[each_t.getText()]
				elif i == 0:
					temp = each_t.getText().split(",")
					# if temp[0] == 'Playoffs':
					# 	writer.writerow(l)
					# 	return game
					temp1 = temp[1].strip().split(' ')
					#print(temp1)
					extra_date = "-".join([temp1[0].strip(), temp1[1].strip(), temp[2].strip()])
					date_obj = dt.strptime(extra_date, "%b-%d-%Y")
					r_date = dt.strftime(date_obj, "%Y%m%d")
				l.append(each_t.getText())
			game.append((r_date,r_team,a_team))		
			writer.writerow(l)
	return game

def playoff():
	for season in range(2001,2018):
		games = get_team_playoff(season)
		for game in games:
			get_box(game[0], game[1], game[2], "box_data_playoff.csv")

if __name__ == '__main__':
	#regular()
	#test()
	playoff()