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
            'name': match[0],
            'surface': match[1],
            'draw_size': match[2],
            'level': match[3],
            'date': parse_date(str(match[4])),
        },
        'round': match[5],
        'score': match[6],
        'winner': {
            'id': match[7],
            'first_name': match[8],
            'last_name': match[9],
            'rank': match[10],
        },
        'loser': {
            'id': match[11],
            'first_name': match[12],
            'last_name': match[13],
            'rank': match[14],
        }
    }
    return new_match

def map_to_mongo_ranking(ranking: tuple, ranking_id: int):
    new_player = {
        '_id': ranking_id,
        'ranking_date': parse_date(str(ranking[0])),
        'rank': ranking[1],
        'player': {
            'id': ranking[2],
            'first_name': ranking[3],
            'last_name': ranking[4],
        },
        'points': ranking[5],
    }
    return new_player

def migrate():

    # Loading data
    connection = sqlite3.connect('database.sqlite')
    cursor = connection.cursor()

    players = cursor.execute('SELECT * FROM players').fetchall()
    matches = cursor.execute('''
        SELECT tourney_name, surface, draw_size, tourney_level, tourney_date, round, score, winner_id, winners.name_first AS winner_first_name, winners.name_last AS winner_last_name, winner_rank, loser_id, losers.name_first AS loser_first_name, losers.name_last AS loser_last_name, loser_rank
        FROM matches INNER JOIN players AS winners ON matches.winner_id = winners.player_id INNER JOIN players losers ON matches.loser_id = losers.player_id;
                             ''').fetchall()
    rankings = cursor.execute('''
        SELECT ranking_date, rank, player, name_first, name_last, points
        FROM rankings INNER JOIN players ON rankings.player = players.player_id;
                              ''').fetchall()

    cursor.close()
    connection.close()

    # Mapping data to collections
    new_players = [map_to_mongo_player(player) for player in players]
    new_matches = [map_to_mongo_match(match, i + 1) for i, match in enumerate(matches)]
    new_rankings = [map_to_mongo_ranking(ranking, i + 1) for i, ranking in enumerate(rankings)]

    # Inserting data to MongoDB
    client = MongoClient('localhost', 27017)

    client.drop_database('tennis-optimized')
    database = client['tennis-optimized']

    players_collection = database['players']
    matches_collection = database['matches']
    rankings_collection = database['rankings']
    
    players_collection.insert_many(new_players)
    matches_collection.insert_many(new_matches)
    rankings_collection.insert_many(new_rankings)

    matches_collection.create_index( [ ('round', 1) ], partialFilterExpression = { 'round': { '$in': [ 'F', 'SF' ] } } )
    matches_collection.create_index( [ ('tournament.level', 1) ], partialFilterExpression = { 'tournament.level': { '$in': [ 'G', 'M', 'F' ] } } )
    matches_collection.create_index( [ ('winner.id', 1) ] )
    matches_collection.create_index( [ ('loser.id', 1) ] )
    rankings_collection.create_index( [ ('rank', 1) ] )

    client.close()

def main():
    migrate()


if __name__ == '__main__':
    main()