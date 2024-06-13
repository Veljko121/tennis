// Prikazati ime i prezime igrača koji je imao najduži niz uzastopnih nedelja na prvom mestu i koji igrač je bio najduže na drugom mestu tokom tog perioda.

db.rankings.aggregate(
[

  // filtriranje samo nedelja sa rankom 1.
  {
    $match: { 
      rank: 1
    }
  },

  // sortiranje
  {
    $sort: 
    {
      player_id: 1,
      "ranking_date": 1
    }
  },

  // grupisanje svih nedelja svakom igracu
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

  // racunanje nizova
  {
    $project: {
      _id: 1, // prikazuje se _id
      streaks: {
        $reduce: {
          input: "$weeks", // for week in weeks
          initialValue: { currentStreak: 1, maxStreak: 1, lastDate: null }, // temp promenljive
          in: {
            $let: {
              vars: {
                lastDate: "$$value.lastDate",
                currentDate: "$$this.date"
              },
              in: {
                $cond: {
                  if: {
                    // ako je tekuci datum tacno nedelju dana posle prethodnog 
                    $eq: [ "$$currentDate", { $dateAdd: { startDate: "$$lastDate", unit: "week", amount: 1 } } ]
                  },
                  then: { // onda se inkrementuje tekuci niz
                    currentStreak: { $add: ["$$value.currentStreak", 1] },
                    maxStreak: {
                      $cond: {
                        if: { $gt: ["$$value.currentStreak", "$$value.maxStreak"] }, // ako je tekuci niz veci od maksimalnog
                        then: "$$value.currentStreak", // onda se maksimalnom nizu daje vrednost tekuceg
                        else: "$$value.maxStreak" // inace vrednost se ne menja
                      }
                    },
                    lastDate: "$$currentDate" // poslednji datum se azurira
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

  // formatiranje ispisa
  {
    $project: {
      player: "$_id",
      max_streak: "$streaks.maxStreak",
      _id: false,
    }
  },
  
  {
    $sort: { max_streak: -1 }
  },

  {
    $limit: 5
  },
  
  {
    $project: {
      player_name: {
        $concat: ["$player.first_name", " ", "$player.last_name"]
      },
      max_streak: 1
    }
  }

]
);