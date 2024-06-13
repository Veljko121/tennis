import sqlite3
from pymongo import MongoClient
import datetime


def parse_date(date_string: str):
    try:
        date_year, date_month, date_day = int(date_string[0:4]), int(date_string[4:6]), int(date_string[6:8])
        date = datetime.datetime(date_year, date_month, date_day)
        return date
    except:
        return None

def map_to_mongo_player(player: tuple):
    new_player = {
        '_id': player[0],
        'first_name': player[1],
        'last_name': player[2],
        'dominant_hand': player[3],
        'dob': parse_date(str(player[4])),
        'country': player[5],
        'height': player[6],
    }
    return new_player

def map_to_mongo_match(match: tuple, match_id: int):
    new_match = {
        '_id': match_id,
        'tournament': {
            'name': match[1],
            'surface': match[2],
            'draw_size': match[3],
            'level': match[4],
            'date': parse_date(str(match[5])),
        },
        'round': match[25],
        'score': match[23],
        'winner_id': match[7],
        'loser_id': match[15]
    }
    return new_match

def map_to_mongo_ranking(ranking: tuple, ranking_id: int):
    new_player = {
        '_id': ranking_id,
        'ranking_date': parse_date(str(ranking[0])),
        'rank': ranking[1],
        'player_id': ranking[2],
        'points': ranking[3],
    }
    return new_player

def migrate():

    # Loading data
    connection = sqlite3.connect('database.sqlite')
    cursor = connection.cursor()

    players = cursor.execute('SELECT * FROM players').fetchall()
    matches = cursor.execute('SELECT * FROM matches WHERE winner_name IS NOT NULL').fetchall()
    rankings = cursor.execute('SELECT * FROM rankings').fetchall()

    cursor.close()
    connection.close()

    # Mapping data to collections
    new_players = [map_to_mongo_player(player) for player in players]
    new_matches = [map_to_mongo_match(match, i + 1) for i, match in enumerate(matches)]
    new_rankings = [map_to_mongo_ranking(ranking, i + 1) for i, ranking in enumerate(rankings)]

    # Inserting data to MongoDB
    client = MongoClient('localhost', 27017)

    client.drop_database('tennis')
    database = client['tennis']

    players_collection = database['players']
    matches_collection = database['matches']
    rankings_collection = database['rankings']

    players_collection.insert_many(new_players)
    matches_collection.insert_many(new_matches)
    rankings_collection.insert_many(new_rankings)

    client.close()

def main():
    migrate()


if __name__ == '__main__':
    main()