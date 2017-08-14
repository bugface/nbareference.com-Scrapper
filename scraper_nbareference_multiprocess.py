from bs4 import BeautifulSoup as bs
import requests as req
import csv
import os
import time
from datetime import datetime as dt
from concurrent.futures import ThreadPoolExecutor, wait
import functools
import sys
import logging
from multiprocessing import cpu_count
from threading import RLock

#global config
lock = RLock()
thread_num = cpu_count()
FORMAT = '%(asctime)-20s %(name)-5s %(levelname)-10s %(message)s'
logging.basicConfig(filename='scrapy_nbarefrence.log',
                    level=logging.INFO, format=FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("scrapy")
user_agent = {
    'User-agent': "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"}
team_rel_abbr = {'New Orleans/Oklahoma City Hornets': 'NOK', 'New Jersey Nets': 'NJN', 'Vancouver Grizzlies': 'VAN', 'Seattle SuperSonics': 'SEA', 'Los Angeles Clippers': 'LAC', 'New Orleans Pelicans': 'NOP', 'Charlotte Hornets': 'CHO', 'Cleveland Cavaliers': 'CLE', 'Miami Heat': 'MIA', 'Phoenix Suns': 'PHO', 'Dallas Mavericks': 'DAL', 'Detroit Pistons': 'DET', 'Charlotte Bobcats': 'CHA', 'Minnesota Timberwolves': 'MIN', 'New Orleans Hornets': 'NOH', 'Indiana Pacers': 'IND', 'Chicago Bulls': 'CHI', 'Oklahoma City Thunder': 'OKC',
                 'Los Angeles Lakers': 'LAL', 'Utah Jazz': 'UTA', 'Memphis Grizzlies': 'MEM', 'Portland Trail Blazers': 'POR', 'LA Clippers': 'LAC', 'Atlanta Hawks': 'ATL', 'Sacramento Kings': 'SAC', 'Philadelphia 76ers': 'PHI', 'Brooklyn Nets': 'BRK', 'Houston Rockets': 'HOU', 'Milwaukee Bucks': 'MIL', 'Boston Celtics': 'BOS', 'Denver Nuggets': 'DEN', 'Orlando Magic': 'ORL', 'Golden State Warriors': 'GSW', 'New York Knicks': 'NYK', 'Toronto Raptors': 'TOR', 'San Antonio Spurs': 'SAS', 'Washington Wizards': 'WAS'}
team_rel_abbr_old = {'New Orleans/Oklahoma City Hornets': 'NOK', 'New Jersey Nets': 'NJN', 'Vancouver Grizzlies': 'VAN', 'Seattle SuperSonics': 'SEA', 'Los Angeles Clippers': 'LAC', 'New Orleans Pelicans': 'NOP', 'Charlotte Hornets': 'CHH', 'Cleveland Cavaliers': 'CLE', 'Miami Heat': 'MIA', 'Phoenix Suns': 'PHO', 'Dallas Mavericks': 'DAL', 'Detroit Pistons': 'DET', 'Charlotte Bobcats': 'CHA', 'Minnesota Timberwolves': 'MIN', 'New Orleans Hornets': 'NOH', 'Indiana Pacers': 'IND', 'Chicago Bulls': 'CHI',
                     'Oklahoma City Thunder': 'OKC', 'Los Angeles Lakers': 'LAL', 'Utah Jazz': 'UTA', 'Memphis Grizzlies': 'MEM', 'Portland Trail Blazers': 'POR', 'LA Clippers': 'LAC', 'Atlanta Hawks': 'ATL', 'Sacramento Kings': 'SAC', 'Philadelphia 76ers': 'PHI', 'Brooklyn Nets': 'BRK', 'Houston Rockets': 'HOU', 'Milwaukee Bucks': 'MIL', 'Boston Celtics': 'BOS', 'Denver Nuggets': 'DEN', 'Orlando Magic': 'ORL', 'Golden State Warriors': 'GSW', 'New York Knicks': 'NYK', 'Toronto Raptors': 'TOR', 'San Antonio Spurs': 'SAS', 'Washington Wizards': 'WAS'}


def output_player_data(future, file):
    data = future.result()

    mode = "w"
    lock.acquire()
    try:
        with open(file, mode, newline="") as f:
            writer = csv.writer(f)
            for each in data:
                writer.writerow(each)
    # except Exception as e:
    #     # logger.info(e)
    finally:
        lock.release()


def scrapy_player_data(game, file):
    time.sleep(1)
    date, team_abbr, away = game
    url_box_score = "http://www.basketball-reference.com/boxscores/{}0{}.html".format(
        date, team_abbr)
    print(url_box_score)
    rep_box = req.get(url_box_score, headers=user_agent)
    soup_box = bs(rep_box.text, "html.parser")

    results = []

    div_id_ht = "all_box_{}_basic".format(team_abbr.lower())
    div_id_at = "all_box_{}_basic".format(away.lower())

    # div for box of away team
    a_div = soup_box.find('div', id=div_id_at)
    a_tbody = a_div.find("tbody")
    a_trs = a_tbody.find_all("tr")
    
    # div for box of home team
    h_div = soup_box.find("div", id=div_id_ht)
    #h_thead = h_div.find("thead")
    #h_ths = h_thead.find_all("th")
    h_tbody = h_div.find("tbody")
    h_trs = h_tbody.find_all("tr")

    trss = (h_trs, a_trs)

    line = []
    #if os.stat(file).st_size == 0:
    if not os.path.isfile(file):
        a_thead = a_div.find("thead")
        a_ths = a_thead.find_all("th")
        for each in a_ths:
            if not (each.getText() == "" or each.getText() == "Basic Box Score Stats"):
                line.append(each.get_text())
        line.append('team')
        line.append('date')
    results.append(line)

    for j, each_trs in enumerate(trss):
        for each in each_trs:
            line = []
            for i, content in enumerate(each.findChildren(['th', 'td'])):
                ctext = content.getText()
                if ctext == "":
                    line.append('0')
                else:
                    line.append(ctext)
            
            if j == 0:
                line.append(team_abbr)
            elif j == 1:
                line.append(away)

            line.append(date)

            results.append(line)

    return results

def scrapy_regular_team_data(season, month, file):
    time.sleep(1)
    url_team_score = "http://www.basketball-reference.com/leagues/NBA_{}_games-{}.html".format(season, month)
    print(url_team_score)

    games = []
    data = []

    r_team = None
    r_date = None

    rep_team = req.get(url_team_score, headers=user_agent)
    soup_team = bs(rep_team.text, "html.parser")
    
    try:
        tbody = soup_team.find("tbody")
        trs = tbody.find_all('tr')

        line = []
        # if os.stat(file).st_size == 0:
        if not os.path.isfile(file):
            thead = soup_team.find("thead")
            ths = thead.find_all("th")
            for each in ths:
                line.append(each.get_text())
        
        for each_tr in trs:
            line = []
            for i, each_t in enumerate(each_tr.findChildren(['th', 'td'])):         
                if i == 4:
                    r_team = team_rel_abbr[each_t.getText()]
                elif i == 2:
                    a_team = team_rel_abbr[each_t.getText()]
                elif i == 0:
                    temp = each_t.getText().split(",")
                    if temp[0] == 'Playoffs':
                        data.append(line)
                        return data, games
                    temp1 = temp[1].strip().split(' ')
                    #print(temp1)
                    extra_date = "-".join([temp1[0].strip(), temp1[1].strip(), temp[2].strip()])
                    date_obj = dt.strptime(extra_date, "%b-%d-%Y")
                    r_date = dt.strftime(date_obj, "%Y%m%d")
                line.append(each_t.getText())
            games.append((r_date,r_team,a_team))     
            data.append(line)
    except Exception as e:
        raise Exception(e)
        games = []
        data = []

    return data, games, season

def output_regular_team_data(future, file):
    data, games, season = future.result()

    if len(data) > 0:
        if os.path.isfile(file):
            mode = "a"
        else:
            mode = "w"

        lock.acquire()
        try:
            with open(file, mode, newline="") as f:
                writer = csv.writer(f)
                for each in data:
                    writer.writerow(each)
        # except Exception as e:
        #     # logger.info(e)
        finally:
            lock.release()

        futures_sub_ = []
        regular_game_outputfile = "data/regularseason_player_data_{}.csv".format(
                    season)
        for game in games:
            with ThreadPoolExecutor(max_workers=thread_num) as executor:
                future_sub_ = executor.submit(
                    scrapy_player_data, game=game, file=regular_game_outputfile)
                future_sub_.add_done_callback(functools.partial(
                    output_player_data, file=regular_game_outputfile))
                futures_sub_.append(future_sub_)
            wait(futures_sub_)

def get_regular_season_data():
    flag = True
    months = ['october', 'november', 'december', 'january', 'february', 'march', 'april']
    start_year, end_year = input_year_range()

    futures_ = []
    try:
        with ThreadPoolExecutor(max_workers=thread_num) as executor:
            for season in range(start_year, end_year):
                regular_team_ouputfile = "data/regular_season_team_data_{}.csv".format(
                        season)
                if os.path.isfile(regular_team_ouputfile):
                    os.remove(regular_team_ouputfile)
                for month in months:
                    # if os.path.isfile(playoff_team_ouputfile):
                    #     os.remove(playoff_team_ouputfile)
                    future_ = executor.submit(
                        scrapy_regular_team_data, season=season, month=month, file=regular_team_ouputfile)
                    future_.add_done_callback(functools.partial(
                        output_regular_team_data, file=regular_team_ouputfile))
                    futures_.append(future_)
                wait(futures_)
    except Exception as e:
        logger.info(e)
        flag = False

    return flag


def output_playoff_team_data(future, file):
    data, games, season = future.result()

    mode = "w"
    lock.acquire()
    try:
        with open(file, mode, newline="") as f:
            writer = csv.writer(f)
            for each in data:
                writer.writerow(each)
    # except Exception as e:
    #     # logger.info(e)
    finally:
        lock.release()

    futures_sub_ = []
    playoffs_game_outputfile = "data/playoff_player_data_{}.csv".format(
                season)
    for game in games:
        with ThreadPoolExecutor(max_workers=thread_num) as executor:
            future_sub_ = executor.submit(
                scrapy_player_data, game=game, file=playoffs_game_outputfile)
            future_sub_.add_done_callback(functools.partial(
                output_player_data, file=playoffs_game_outputfile))
            futures_sub_.append(future_sub_)
        wait(futures_sub_)


def scrapy_playoff_team_data(season, file):
    time.sleep(1)
    url_team_score = "http://www.basketball-reference.com/playoffs/NBA_{}_games.html".format(
        season)
    print(url_team_score)

    if season < 2010:
        team_r_abbr = team_rel_abbr_old
    else:
        team_r_abbr = team_rel_abbr

    data = []
    line = []
    games = []

    r_team = None
    r_date = None

    rep_team = req.get(url_team_score, headers=user_agent)
    soup_team = bs(rep_team.text, "html.parser")

    tbody = soup_team.find("tbody")
    trs = tbody.find_all('tr')

    
    #if os.stat(file).st_size == 0:
    if not os.path.isfile(file):
        thead = soup_team.find("thead")
        ths = thead.find_all("th")
        for each in ths:
            line.append(each.get_text())
        data.append(line)

    for each_tr in trs:
        line = []
        for i, each_t in enumerate(each_tr.findChildren(['th', 'td'])):
            if i == 4:
                r_team = team_r_abbr[each_t.getText()]
            elif i == 2:
                a_team = team_r_abbr[each_t.getText()]
            elif i == 0:
                temp = each_t.getText().split(",")
                # if temp[0] == 'Playoffs':
                #   writer.writerow(l)
                #   return game
                temp1 = temp[1].strip().split(' ')
                # print(temp1)
                extra_date = "-".join([temp1[0].strip(),
                                       temp1[1].strip(), temp[2].strip()])
                date_obj = dt.strptime(extra_date, "%b-%d-%Y")
                r_date = dt.strftime(date_obj, "%Y%m%d")
            line.append(each_t.getText())

        data.append(line)
        games.append((r_date, r_team, a_team))

    return data, games, season


def get_playoff_data():
    flag = True
    start_year, end_year = input_year_range()

    futures_ = []
    try:
        with ThreadPoolExecutor(max_workers=thread_num) as executor:
            for season in range(start_year, end_year):
                playoff_team_ouputfile = "data/playoff_team_data_{}.csv".format(
                    season)
                # if os.path.isfile(playoff_team_ouputfile):
                #     os.remove(playoff_team_ouputfile)
                future_ = executor.submit(
                    scrapy_playoff_team_data, season=season, file=playoff_team_ouputfile)
                future_.add_done_callback(functools.partial(
                    output_playoff_team_data, file=playoff_team_ouputfile))
                futures_.append(future_)
            wait(futures_)
    except Exception as e:
        logger.info(e)
        flag = False

    return flag

def input_year_range():
    try:
        current_year = int(time.strftime("%Y"))
        start_year = int(input("start year: (format YYYY)"))
        end_year = int(input("end year: (format YYYY)"))
    except Exception as e:
        print(e)
        sys.exit(1)

    if start_year >= end_year or start_year > current_year or end_year > current_year:
        print("input error!")
        sys.exit(1)
    else:
        return start_year, end_year

def main():
    job = input("regular season (r)? or playoffs (p)?")
    flag = None

    directory = "data"
    if not os.path.exists(directory):
        os.makedirs(directory)

    if job == "r":
        flag = get_regular_season_data()
    elif job == "p":
        flag = get_playoff_data()
    else:
        print("input error!")
        sys.exit(1)

    if flag:
        print("done")
    else:
        print("something wrong check log for detail.")


if __name__ == '__main__':
    main()
