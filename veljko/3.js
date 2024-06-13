// Prikazati 5 turnira koji imaju najviše višestrukih šampiona koji su bili na prvom mestu barem 250 nedelja.

db.matches.aggregate(
[
  {
    $match: {
      round: "F" // samo finalni mecevi
    }
  },

  // grupise se po nazivu turnira i pobedniku
  {
    $group: {
      _id: {
        tournament: "$tournament.name",
        winner_id: "$winner_id"
      },
      titles: {
        // prebrojavanje titula
        $sum: 1
      }
    }
  },

  // formatiranje podataka
  {
    $project: {
      tournament: "$_id.tournament",
      _id: false,
      winner: "$_id.winner_id",
      titles: true
    }
  },

  // spajanje sa igracima
  {
    $lookup: {
      from: "players",
      localField: "winner",
      foreignField: "_id",
      as: "winner"
    }
  },

  // formatiranje
  {
    $addFields: {
      winner: {
        $first: "$winner"
      }
    }
  },

  // spajanje rankinga, tj. nedelja na broju 1
  {
    $lookup: {
      from: "rankings",
      let: {
        playerId: "$winner._id"
      },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: [ "$player_id", "$$playerId" ] },
                { $eq: ["$rank", 1] }
              ]
            }
          }
        }
      ],
      as: "rankings"
    }
  },

  {
    $sort: {
      titles: -1
    }
  },

  {
    $project:
      {
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