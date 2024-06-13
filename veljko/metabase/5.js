[
  
  {
    $match: {
      _id: 104925
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
              $or: [
                { $eq: [ "$winner.id", "$$playerId" ] },
                { $eq: [ "$loser.id", "$$playerId" ] }
              ]
            }
          }
        }
      ],
      as: "matches"
    }
  },

  {
    $unwind: "$matches"
  },

  {
    $group: {
      _id: {
        playerId: "$_id",
        firstName: "$first_name",
        lastName: "$last_name",
        surface: "$matches.tournament.surface"
      },
      wins: {
        $sum: {
          $cond: {
            if: { $eq: [ "$matches.winner.id", "$_id" ] },
            then: 1,
            else: 0
          }
        }
      },
      losses: {
        $sum: {
          $cond: {
            if: { $eq: [ "$matches.loser.id", "$_id" ] },
            then: 1,
            else: 0
          }
        }
      },
      totalMatches: {
        $sum: 1
      }
    }
  },
  
  {
    $project: {
      _id: 0,
      player_id: "$_id.playerId",
      first_name: "$_id.firstName",
      last_name: "$_id.lastName",
      surface: "$_id.surface",
      winPercentage: {
        $cond: {
          if: { $eq: [ "$totalMatches", 0 ] },
          then: 0,
          else: { $multiply: [{ $divide: ["$wins", "$totalMatches"] }, 100] }
        }
      },
      lossPercentage: {
        $cond: {
          if: { $eq: [ "$totalMatches", 0 ] },
          then: 0,
          else: { $multiply: [{ $divide: ["$losses", "$totalMatches"] }, 100] }
        }
      },
      totalMatches: 1
    }
  },

]