db.matches.createIndex(
    { 'round': 1 },
    { partialFilterExpression: { 'round': { '$in': [ 'F', 'SF' ] } } }
);

db.matches.createIndex(
    { 'tournament.level': 1 },
    { partialFilterExpression: { 'tournament.level': { '$in': [ 'G', 'M', 'F' ] } } }
);

db.matches.createIndex(
    { 'winner.id': 1 }
);

db.matches.createIndex(
    { 'loser.id': 1 }
);

db.rankings.createIndex(
    { 'rank': 1 }
);

db.rankings.createIndex(
    { 'player.id': 1 }
);

db.rankings.createIndex(
    { 'ranking_date': 1 }
);
