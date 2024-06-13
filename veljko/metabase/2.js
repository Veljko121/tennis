[

  {
    $match: { 
      rank: 1
    }
  },

  {
    $sort: 
    {
      player_id: 1,
      "ranking_date": 1
    }
  },

  {
    $group: {
      _id: "$player",
      weeks: {
        $push: {
          date: "$ranking_date",
          rank: "$rank"
        }
      }
    }
  },

  {
    $project: {
      _id: 1,
      streaks: {
        $reduce: {
          input: "$weeks",
          initialValue: { currentStreak: 1, maxStreak: 1, lastDate: null },
          in: {
            $let: {
              vars: {
                lastDate: "$$value.lastDate",
                currentDate: "$$this.date"
              },
              in: {
                $cond: {
                  if: {
                    $eq: [ "$$currentDate", { $dateAdd: { startDate: "$$lastDate", unit: "week", amount: 1 } } ]
                  },
                  then: {
                    currentStreak: { $add: ["$$value.currentStreak", 1] },
                    maxStreak: {
                      $cond: {
                        if: { $gt: ["$$value.currentStreak", "$$value.maxStreak"] },
                        then: "$$value.currentStreak",
                        else: "$$value.maxStreak"
                      }
                    },
                    lastDate: "$$currentDate"
                  },
                  else: {
                    currentStreak: 1,
                    maxStreak: "$$value.maxStreak",
                    lastDate: "$$currentDate"
                  }
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
      player: "$_id",
      maxStreak: "$streaks.maxStreak",
      _id: false,
    }
  },
  
  {
    $sort: { maxStreak: -1 }
  },

  {
    $limit: 5
  },
  
  {
    $addFields: {
      player_name: {
        $concat: ["$player.first_name", " ", "$player.last_name"]
      }
    }
  }

]