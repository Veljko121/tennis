[

  {
    $match: {
      _id: {
        $in: [103819, 104745, 104925]
      }
    }
  },

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
                { $eq: ["$winner.id", "$$playerId"] },
                { $eq: ["$round", "F"] }
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
      as: "won_tournaments"
    }
  },

  {
    $project: {
      first_name: 1,
      last_name: 1,
      tournaments_during_no1: {
        $filter: {
          input: "$won_tournaments",
          as: "tournament",
          cond: {
            $anyElementTrue: {
              $map: {
                input: "$ranking_dates",
                as: "ranking",
                in: {
                  $and: [
                    {
                      $gte: [ "$$tournament.tournament_date", "$$ranking.ranking_date" ]
                    },
                    {
                      $lte: [
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