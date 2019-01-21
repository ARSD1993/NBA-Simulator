# NBA-Simulator

  This application collections player data and then uses it to predict the outcome of scheduled NBA basketball games. It works as follows:

    1) The application first collects data from all players on all teams. To decrease the amount of times the application does this, player data will only be collected once a week.

    2) Next the application will then collect all of todays scheduled games from the NBA and using an algorithm predict the winner using the data from part 1.

    3) After collecting and predicting todays scheduled games the application will then collect the previous days result where it can then be compared with the applications prediction. All this data can be found in the scheduled games database table.

# Todo

  1) The data collection does not account for players that are injured and not able to play. The application will need to be updated to collect a players injury status. This will require another column in the player tables and will require player data collection to be daily so we always know whos playing.
  
  2) Update the algorithm to be better. Too simple at the moment need to look into advanced states and use career player data. May also want to look into keeping a list of specific players who should always have a high rating (aka superstars).
  
  3) Database configuration should not be hardcoded. Look into making a front end so user can specify this information. And be able to see past results easier.

