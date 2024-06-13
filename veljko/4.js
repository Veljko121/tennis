// Prikazati GOAT listu od 5 igrača koji se rangiraju na osnovu goat_score-a, a goat_score se računa kao:
// goat_score = big_titles + weeks_at_no1 / 20.
// Velike titule su sve titule nivoa Grand Slam, Masters i ATP Finals.


// pocinje se od rankinga, jer je neophodno filtrirati nepotrebne, sto je 99% torki,
// znacajno se ubrzava nego ako se pocne od meceva
db.rankings.aggregate(
[
  // samo rankinzi za prvo mesto, od 3 miliona ostaje 2.5 hiljada
  {
    $match: {
      rank: 1
    }
  },
  // grupisu se nedelje po igracu i agregiraju u broj nedelja na prvom mestu
  {
    $group: {
      _id: "$player_id",
      weeks: {
        $sum: 1
      }
    }
  },
  // spajanje sa mecevima, ali samo finalnim na turnirima nivoa Grand Slam, Masters ili ATP Finals
  // u kome je igrac pobedio
  {
    $lookup: {
      from: "matches",
      let: {
        playerId: "$_id"
      },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: [ "$winner_id", "$$playerId" ] },
                { $eq: ["$round", "F"] },
                { $in: [ "$tournament.level", ["M", "G", "F"]] }
              ]
            }
          }
        }
      ],
      as: "matches"
    }
  },
  // brojanje velikih titula
  {
    $addFields: {
      big_titles: {
        $size: "$matches"
      }
    }
  },

  // izbacivanje meceva, nepotrebni su
  {
    $project: {
      matches: false
    }
  },

  // racunanje goat_score-a
  {
    $addFields: {
      goat_score: {
        $add: [
          "$big_titles",
          {
            $divide: ["$weeks", 20]
          }
        ]
      }
    }
  },

  // spajanje sa igracima
  {
    $lookup: {
      from: "players",
      localField: "_id",
      foreignField: "_id",
      as: "player"
    }
  },

  // uredjivanje ispisa
  {
    $addFields: {
      player: {
        $first: "$player"
      }
    }
  },

  // izbacivanje _id
  {
    $project: {
      _id: false
    }
  },

  // sortiranje po goat_score-u
  {
    $sort: {
      goat_score: -1
    }
  },

  // top 5
  {
    $limit: 5
  }
]
);