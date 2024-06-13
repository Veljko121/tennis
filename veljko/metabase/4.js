[
  
  {
    $match: {
      rank: 1
    }
  },

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

  {
    $addFields: {
      big_titles: {
        $size: "$matches"
      }
    }
  },

  {
    $project: {
      matches: false
    }
  },

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

  {
    $project: {
      _id: false
    }
  },

  {
    $sort: {
      goat_score: -1
    }
  },

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