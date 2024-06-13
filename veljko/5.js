// Prikazati ime, prezime i procente uspešnosti igrača (zadatog preko _id) na svim podlogama.

db.matches.aggregate(
[
  {
    $match: {
      _id: 104925  // id igraca koji nas zanima
    }
  },

  // spajanje sa kolekcijom matches, dodaje se lista
  // meceva u kojima je posmatrani igrac ucestvovao
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
              $or: [ // igrac je ili pobedio ili izgubio
                { $eq: [ "$winner_id", "$$playerId" ] },
                { $eq: [ "$loser_id", "$$playerId" ] }
              ]
            }
          }
        }
      ],
      as: "matches"
    }
  },

  {
    $unwind: "$matches"  // razdvajamo elemente iz liste meceva, svaki da bude poseban
  },

  // grupisu se mecevi po igracu i podlozi
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
          $cond: { // broje se samo mecevi u kojima je pobedio
            if: { $eq: [ "$matches.winner_id", "$_id" ] },
            then: 1,
            else: 0
          }
        }
      },
      losses: {
        $sum: {
          $cond: { // broje se samo mecevi u kojima je izgubio
            if: { $eq: [ "$matches.loser_id", "$_id" ] },
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
  
  // formatiranje ispisa i racunanje procenata
  {
    $project: {
      _id: 0,
      player_id: "$_id.playerId",
      first_name: "$_id.firstName",
      last_name: "$_id.lastName",
      surface: "$_id.surface",
      winPercentage: { // racunanje procenta pobeda
        $cond: {
          if: { $eq: [ "$totalMatches", 0 ] },
          then: 0,
          else: { $multiply: [{ $divide: ["$wins", "$totalMatches"] }, 100] }
        }
      },
      lossPercentage: { // racunanje procenta poraza
        $cond: {
          if: { $eq: [ "$totalMatches", 0 ] },
          then: 0,
          else: { $multiply: [{ $divide: ["$losses", "$totalMatches"] }, 100] }
        }
      },
      totalMatches: 1
    }
  },

  // konacno grupisanje
  {
    $group: {
      _id: {
        player_id: "$player_id",
        first_name: "$first_name",
        last_name: "$last_name"
      },
      surface_stats: {
        $push: {
          surface: "$surface",
          win_percentage: "$winPercentage",
          loss_percentage: "$lossPercentage"
        }
      }
    }
  },

  // konacno formatiranje
  {
    $project: {
      _id: 0,
      player_id: "$_id.player_id",
      first_name: "$_id.first_name",
      last_name: "$_id.last_name",
      surface_stats: 1
    }
  }
]
);