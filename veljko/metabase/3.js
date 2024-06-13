[
  
  {
    $match: {
      round: "F"
    }
  },
  
  {
    $group: {
      _id: {
        tournament: "$tournament.name",
        winner: {
          id: "$winner.id",
          first_name: "$winner.first_name",
          last_name: "$winner.last_name",
        }
      },
      titles: {
        $sum: 1
      }
    }
  },
  {
    $sort: {
      titles: -1
    }
  },

  {
    $project: {
      tournament: "$_id.tournament",
      _id: false,
      winner: "$_id.winner",
      titles: true
    }
  },

  {
    $lookup: {
      from: "rankings",
      let: {
        playerId: "$winner.id"
      },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                {
                  $eq: [
                    "$player.id",
                    "$$playerId"
                  ]
                },
                {
                  $eq: ["$rank", 1]
                }
              ]
            }
          }
        }
      ],
      as: "rankings"
    }
  },

  {
    $project: {
      weeks_at_no1: {
        $size: "$rankings"
      },
      titles: true,
      tournament: true,
      winner: true
    }
  },

  {
    $match: {
      weeks_at_no1: {
        $gte: 250
      }
    }
  },

  {
    $limit: 5
  }

]