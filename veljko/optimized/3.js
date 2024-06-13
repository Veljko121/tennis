// Prikazati 5 turnira koji imaju najviše višestrukih šampiona koji su bili na prvom mestu barem 250 nedelja.

db.matches.aggregate(
[
  {
    $match: {
      round: "F"
    }
  },
  
  // grupise se po nazivu turnira i pobedniku
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
        // prebrojavanje titula
        $sum: 1
      }
    }
  },
  {
    $sort: {
      titles: -1
    }
  },

  // formatiranje podataka
  {
    $project: {
      tournament: "$_id.tournament",
      _id: false,
      winner: "$_id.winner",
      titles: true
    }
  },

  // spajanje rankinga, tj. nedelja na broju 1
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

  // samo igraci koji imaju barem 250 nedelja na prvom mestu
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
);