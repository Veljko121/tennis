// Prikazati ime, prezime i broj osvojenih turnira igrača (zadatog preko _id) dok je bio na prvom mestu na ATP ranking listi.

db.players.aggregate(
[

  {
    $match: {
      _id: {
        $in: [103819, 104745, 104925]
      }
    }
  },

  // spajanje sa kolekcijom rankinga, dodaje se lista
  // nedelja kada je igrac bio na prvom mestu
  {
    $lookup: { 
      from: "rankings",
      let: {
        playerId: "$_id"
      },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ["$player.id", "$$playerId"] },
                { $eq: ["$rank", 1] }
              ]
            }
          }
        },
        { $project: { _id: 0, ranking_date: 1, rank: 1 } }
      ],
      as: "ranking_dates"
    }
  },

  // sada se spaja sve to sa mecevima finala u kojima je igrac pobedio
  // i ti mecevi se dodaju u odvojenu listu
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
                { $eq: ["$winner.id", "$$playerId"] }, // mecevi u kojima je igrac pobedio
                { $eq: ["$round", "F"] } // samo mecevi finala
              ]
            }
          }
        },
        {
          $project: {
            _id: 0,
            tournament_date: "$tournament.date"
          }
        }
      ],
      as: "won_tournaments" // lista svih finalnih meceva u kojim je igrac pobedio
    }
  },

  {
    $project: {
      first_name: 1,
      last_name: 1,
      tournaments_during_no1: {
        $filter: { // filtriranje meceva tako da budu samo oni koji su bili dok je igrac bio prvi na svetu
          input: "$won_tournaments", // turniri
          as: "tournament", // for tournament in won_tournaments
          cond: { // filter
            $anyElementTrue: {
              $map: {
                input: "$ranking_dates",
                as: "ranking", // for ranking in ranking_dates
                in: {
                  $and: [
                    {
                      // datum pocetka turnira mora biti veci ili jednak od trenutno posmatranog rankinga
                      $gte: [ "$$tournament.tournament_date", "$$ranking.ranking_date" ]
                    },
                    {
                      $lte: [ // datum pocetka turnira mora biti manji ili jednak od naredne nedelje
                        "$$tournament.tournament_date",
                        { $dateAdd: { startDate: "$$ranking.ranking_date", unit: 'week', amount: 1 } }
                      ]
                    }
                  ]
                }
              }
            }
          }
        }
      }
    }
  },

  {
    $project: {
      _id: 0,
      full_name: {
        $concat: ['$first_name', ' ', '$last_name']
      },
      num_tournaments_during_no1: { $size: "$tournaments_during_no1" }
    }
  }
]

);