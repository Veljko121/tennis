// Prikazati GOAT listu od 5 igrača koji se rangiraju na osnovu goat_score-a, a goat_score se računa kao:
// goat_score = big_titles + weeks_at_no1 / 20.
// Velike titule su sve titule nivoa Grand Slam, Masters i ATP Finals.

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
      _id: {
        player: {
          id: "$player.id",
          first_name: "$player.first_name",
          last_name: "$player.last_name"
        }
      },
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
        playerId: "$_id.player.id"
      },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: [ "$winner.id", "$$playerId" ] },
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
      },
      player: "$_id.player"
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
  },

  {
    $addFields: {
      player_name: {
        $concat: ['$player.first_name', ' ', '$player.last_name']
      }
    }
  }

]
);