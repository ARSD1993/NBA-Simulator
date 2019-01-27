# NBA-Simulator

  This application collections player data and then uses it to predict the outcome of scheduled NBA basketball games. It works as follows:

    1) The application first collects data from all players on all teams. To decrease the amount of times the application does this, player data will only be collected once a week.

    2) Next the application will then collect all of todays scheduled games from the NBA and using an algorithm predict the winner using the data from part 1.

    3) After collecting and predicting todays scheduled games the application will then collect the previous days result where it can then be compared with the applications prediction. All this data can be found in the scheduled games database table.

# Todo

  1) The application has been updated to discard players who are injured and will not play however it seems that some players who are out for the season do not have an injury status (as an example John Wall), might need to add some player status manually. Still need to update ho often application should collect player data as injury status could change at any point. Might have to make it hourly.
  
  2) Update the algorithm to be better. Too simple at the moment need to look into advanced states and use career player data. May also want to look into keeping a list of specific players who should always have a high rating (aka superstars).
  
  3) Database configuration should not be hardcoded. Look into making a front end so user can specify this information. And be able to see past results easier.

