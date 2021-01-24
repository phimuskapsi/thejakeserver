const mariadb = require('mariadb');
const moment  = require('moment');
const express = require('express');
const router  = express.Router();
const pool    = mariadb.createPool(require('../config.json'));
var error   = { message: 'Error!', code: 0 };

async function queryDB(query, params, res){
  let conn;

  if (typeof(query) === 'undefined'){
    return false;
  }

  //eslint-disable-next-line
  //console.log('query', query);
  //eslint-disable-next-line
  //console.log('params', params);

  try {
    conn = await pool.getConnection();
    var rows = [];

    if(params !== false){
      rows = await conn.query(query, params);
    } else {
      rows = await conn.query(query);
    }

    
    //eslint-disable-next-line
    //console.log('rows', rows);

    conn.end();
    if(Array.isArray(rows)){      
      return rows;
    }

    return {
      success: rows.affectedRows > 0 && rows.warningStatus === 0,
      id: rows.insertId
    };

  } catch (err) {
    //eslint-disable-next-line
    console.log('err', err);
    res.json(err);
  } finally {
    if (conn) conn.end();
  }
}

router.get('/get/passing/:season/:week', async (req, res) => {
  try {
    var seasonsQuery = `SELECT g.* FROM games`;

    var seasons = await queryDB(seasonsQuery, [], res);
  } catch (err) {
    res.status(500).json(error);
  }
});

router.get('/get/games/all', async (req, res) => {
  try {
    var gamesQuery = `SELECT g.* FROM nfl.games g WHERE g.season = 2020 ORDER BY g.gameId `;
    var games = await queryDB(gamesQuery, [], res);

    res.json({done: true, success: true, games: games });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.post('/add/game/main', async (req, res) => {
  try {
    var gameData = req.body;
    var queryData = getKeysAndValues(gameData);

    let query = ` INSERT INTO nfl.games 
                    (${queryData.keysSQL}) 
                  VALUES (${queryData.valsSQL});`;

    await queryDB(query, queryData.params, res);  
    res.json({done: true, success: true });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.post('/add/game/aggregate', async (req, res) => {
  try {
    var teamData = req.body;
    var queryData = getKeysAndValues(teamData);

    let query = ` INSERT INTO nfl.games_aggregate 
                    (${queryData.keysSQL}) 
                  VALUES (${queryData.valsSQL});`;

    await queryDB(query, queryData.params, res);  
    res.json({done: true, success: true });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.post('/add/game/defensive', async (req, res) => {
  try {
    var teamData = req.body;
    var queryData = getKeysAndValues(teamData);

    let query = ` INSERT INTO nfl.games_defensive 
                    (${queryData.keysSQL}) 
                  VALUES (${queryData.valsSQL});`;

    await queryDB(query, queryData.params, res);  
    res.json({done: true, success: true });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.post('/add/game/fumbles', async (req, res) => {
  try {
    var teamData = req.body;
    var queryData = getKeysAndValues(teamData);

    let query = ` INSERT INTO nfl.games_fumbles 
                    (${queryData.keysSQL}) 
                  VALUES (${queryData.valsSQL});`;

    await queryDB(query, queryData.params, res);  
    res.json({done: true, success: true });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.post('/add/game/kicking', async (req, res) => {
  try {
    var teamData = req.body;
    var queryData = getKeysAndValues(teamData);

    let query = ` INSERT INTO nfl.games_kicking
                    (${queryData.keysSQL}) 
                  VALUES (${queryData.valsSQL});`;

    await queryDB(query, queryData.params, res);  
    res.json({done: true, success: true });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.post('/add/game/passing', async (req, res) => {
  try {
    var teamData = req.body;
    var queryData = getKeysAndValues(teamData);

    let query = ` INSERT INTO nfl.games_passing
                    (${queryData.keysSQL}) 
                  VALUES (${queryData.valsSQL});`;

    await queryDB(query, queryData.params, res);  
    res.json({done: true, success: true });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.post('/add/game/punting', async (req, res) => {
  try {
    var teamData = req.body;
    var queryData = getKeysAndValues(teamData);

    let query = ` INSERT INTO nfl.games_punting
                    (${queryData.keysSQL}) 
                  VALUES (${queryData.valsSQL});`;

    await queryDB(query, queryData.params, res);  
    res.json({done: true, success: true });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.post('/add/game/receiving', async (req, res) => {
  try {
    var teamData = req.body;
    var queryData = getKeysAndValues(teamData);

    let query = ` INSERT INTO nfl.games_receiving 
                    (${queryData.keysSQL}) 
                  VALUES (${queryData.valsSQL});`;

    await queryDB(query, queryData.params, res);  
    res.json({done: true, success: true });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.post('/add/game/return', async (req, res) => {
  try {
    var teamData = req.body;
    var queryData = getKeysAndValues(teamData);

    let query = ` INSERT INTO nfl.games_return 
                    (${queryData.keysSQL}) 
                  VALUES (${queryData.valsSQL});`;

    await queryDB(query, queryData.params, res);  
    res.json({done: true, success: true });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.post('/add/game/rushing', async (req, res) => {
  try {
    var teamData = req.body;
    var queryData = getKeysAndValues(teamData);

    let query = ` INSERT INTO nfl.games_rushing
                    (${queryData.keysSQL}) 
                  VALUES (${queryData.valsSQL});`;

    await queryDB(query, queryData.params, res);  
    res.json({done: true, success: true });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.post('/add/jakes/', async (req, res) => {
  try {
    // Even though we have *some* data back to 2001, not all. 2005 seems to be where this query starts working
    for(var s=2006;s<2020;s++) {
      let homePlayersQuery = `SELECT g.season, g.week, g.gameDate, g.gameId, score.homePointTotal, player.nflId, player.displayName, player.birthDate, team.nick,
                                pass.playerId, pass.passingAttempts, pass.passingCompletions, pass.passingInterceptions, pass.passingSacked, team.teamId,
                                IF(g.season > 2008, fumble.fumblesLost, (fumble.fumbles - (fumble.teammateFumbleRecovery + fumble.opponentFumbleRecovery + fumble.fumblesOutbounds))) as calcFumLost
                              FROM nfl.games g
                                JOIN nfl.games_passing pass ON pass.gameId = g.gameId AND pass.teamId = g.homeTeamId
                                JOIN nfl.games_aggregate agg ON agg.gameId = g.gameId AND agg.teamId = g.homeTeamId                            
                                JOIN nfl.games_fumbles fumble ON fumble.gameId = g.gameId AND fumble.playerId = pass.playerId                            
                                JOIN nfl.games_score score ON score.gameId = g.gameId
                                JOIN nfl.players player ON player.nflId = pass.playerId and player.season = g.season
                                JOIN nfl.teams team ON team.teamId = pass.teamId
                              WHERE pass.passingAttempts > 7 AND pass.season = ${s} AND g.homeWin = 0 AND player.position = 'QB'
                              GROUP BY g.season, g.week, pass.gameId, pass.playerId
                              ORDER BY g.season, g.week`;

      let vPlayersQuery =   `SELECT g.season, g.week, g.gameDate, g.gameId, score.visitorPointTotal, player.nflId, player.displayName, player.birthDate, team.nick,
                              pass.playerId, pass.passingAttempts, pass.passingCompletions, pass.passingInterceptions, pass.passingSacked, team.teamId,
                              IF(g.season > 2008, fumble.fumblesLost, (fumble.fumbles - (fumble.teammateFumbleRecovery + fumble.opponentFumbleRecovery + fumble.fumblesOutbounds))) as calcFumLost
                            FROM nfl.games g
                              JOIN nfl.games_passing pass ON pass.gameId = g.gameId AND pass.teamId = g.visitorTeamId
                              JOIN nfl.games_aggregate agg ON agg.gameId = g.gameId AND agg.teamId = g.visitorTeamId                            
                              JOIN nfl.games_fumbles fumble ON fumble.gameId = g.gameId AND fumble.playerId = pass.playerId                            
                              JOIN nfl.games_score score ON score.gameId = g.gameId
                              JOIN nfl.players player ON player.nflId = pass.playerId and player.season = g.season
                              JOIN nfl.teams team ON team.teamId = pass.teamId
                            WHERE pass.passingAttempts > 7 AND pass.season = ${s} AND g.visitorWin = 0 AND player.position = 'QB'
                            GROUP BY g.season, g.week, pass.gameId, pass.playerId
                            ORDER BY g.season, g.week`;          
                            
      // eslint-disable-next-line
      //console.log('homePlayersQuery:', homePlayersQuery);                      

      let homeGameStats = await queryDB(homePlayersQuery, [], res);
      let vGameStats = await queryDB(vPlayersQuery, [], res);
      var jakes = {};
      var fumLost = 0;
      var ints = 0;
      var multiplier = 0;
      var jakeScore = 0;
      var birthDate = '';
      var gameDate = '';
      var isBirthday = false

      // Init season in jakes
      jakes[s] = {};

      let jakeCount = 0;
      // eslint-disable-next-line
      console.log('Starting jake home team losers processing for season: ', s);
      
      delete homeGameStats.meta;
      delete vGameStats.meta;
      // eslint-disable-next-line
      // console.log('Home game stats:', homeGameStats);
      //return;

      for(var h=0;h<homeGameStats.length;h++) {
        let homeGamePlayer = homeGameStats[h];
        fumLost = homeGamePlayer.calcFumLost < 0 ? 0 : homeGamePlayer.calcFumLost;
        ints = homeGamePlayer.passingInterceptions;
        multiplier = 1/6;
        jakeScore = (((fumLost + ints) * multiplier) * 100).toFixed(2);
        birthDate = moment(new Date(homeGamePlayer.birthDate));
        gameDate = moment(new Date(homeGamePlayer.gameDate));
        isBirthday = birthDate.format('MM-DD') === gameDate.format('MM-DD');

        if (jakeScore > 0) {        
          jakeCount++;
          if(typeof(jakes[homeGamePlayer.season][homeGamePlayer.week]) === "undefined") jakes[homeGamePlayer.season][homeGamePlayer.week] = [];

          jakes[homeGamePlayer.season][homeGamePlayer.week].push({
            playerId: homeGamePlayer.playerId,
            fumblesLost: fumLost,
            interceptions: ints,
            jakeScore: jakeScore,
            jakeRank: 0,
            isBirthday: isBirthday,
            season: homeGamePlayer.season, 
            week: homeGamePlayer.week,
            gameId: homeGamePlayer.gameId,
            teamId: homeGamePlayer.teamId
          });
        }
      }
      
      // eslint-disable-next-line
      console.log('Starting jake visitor team losers processing for season: ', s);
      for(var v=0;v<vGameStats.length;v++) {
        let vGamePlayer = vGameStats[v];
        fumLost = vGamePlayer.calcFumLost < 0 ? 0 : vGamePlayer.calcFumLost;
        ints = vGamePlayer.passingInterceptions;
        multiplier = 1/6;
        jakeScore = (((fumLost + ints) * multiplier) * 100).toFixed(2);
        birthDate = moment(new Date(vGamePlayer.birthDate));
        gameDate = moment(new Date(vGamePlayer.gameDate));
        isBirthday = birthDate.format('MM-DD') === gameDate.format('MM-DD');

        if (jakeScore > 0) {
          jakeCount++;
          if(typeof(jakes[vGamePlayer.season][vGamePlayer.week]) === "undefined") jakes[vGamePlayer.season][vGamePlayer.week] = [];

          jakes[vGamePlayer.season][vGamePlayer.week].push({
            playerId: vGamePlayer.playerId,
            fumblesLost: fumLost,
            interceptions: ints,
            jakeScore: jakeScore,
            jakeRank: 0,
            isBirthday: isBirthday,
            season: vGamePlayer.season, 
            week: vGamePlayer.week,
            gameId: vGamePlayer.gameId,
            teamId: vGamePlayer.teamId
          });
        }
      }

      //let jakeCount = 0;
      // eslint-disable-next-line
      console.log(`Starting jake sorting for season: ${s}, number of jakes to process ${jakeCount}`);
      
      if (Object.keys(jakes).length > 0) {
        for(var season in jakes) {
          for(var seasonWeeks in jakes[season]) {
            let playersForWeek = jakes[season][seasonWeeks];
            let jakeRankings = {
              firstPlace: {
                score: 0,
                val: 1,
                players: []
              },
              secondPlace: {
                score: 0,
                val: 2,
                players: []
              },
              thirdPlace: {
                score: 0,
                val: 3,
                players: []
              },
              otherPlace: {
                score: 0,
                val: 4,
                players: []
              }              
            };

            playersForWeek.sort((a, b) => {
              return a.jakeScore > b.jakeScore ? -1 : 1
            });

            // eslint-disable-next-line
            //console.log('playersForWeek: ', playersForWeek);
            //return;
                      
            // eslint-disable-next-line
            //console.log(`Starting jake ranking for season: ${s}, week: ${seasonWeeks}`);
            for(let p=0;p<playersForWeek.length;p++){
              let playerData = playersForWeek[p];

              for(let rank in jakeRankings) {
                let ranking = jakeRankings[rank];
                
                if (ranking.score === 0) {
                  ranking.score = playerData.jakeScore;                  
                  ranking.players.push(playerData.playerId);
                  jakeRankings[rank] = ranking;
                  playerData.jakeRank = ranking.val;       
                  break;
                }               
                
                if (ranking.score > 0 && playerData.jakeScore === ranking.score){
                  playerData.jakeRank = ranking.val;  
                  ranking.players.push(playerData.playerId);  
                  jakeRankings[rank] = ranking;
                  break;
                }                
              }
              
              playersForWeek[p] = playerData;
            }

            // So at this point we should have jakes all sorted by score in one object "playersForWeek"
            
            //eslint-disable-next-line
            //console.log(`Jake Rankings:`, jakeRankings);
            //return;
            
            for(let p=0;p<playersForWeek.length;p++){
              let playerData = playersForWeek[p];
              let playerQueryData = getKeysAndValues(playerData);
              let playerInsertQuery = ` INSERT INTO nfl.games_jakes  (${playerQueryData.keysSQL}) 
                                        VALUES (${playerQueryData.valsSQL});`;

              await queryDB(playerInsertQuery, playerQueryData.params, res);  
            }

            // We should also have a full 'rankings' set. NOW we can insert things into the jake standings table and jakes table
            // Rankings should contain all the players jake rankings for the week.
            // When the player already exists, it will instead increment the proper place by 1, and total jakes by 1
            // We then have a count of how often this has happened for each player over their entire career. Historical info can also come from jakes 
            // eslint-disable-next-line
            //console.log(`Inserting jakes rankings: ${s}, week: ${seasonWeeks}`);
            for(let rank in jakeRankings) {
              let ranking = jakeRankings[rank];
              for (var rp=0;rp<ranking.players.length;rp++) {
                let rplId = ranking.players[rp];
                let rankingInsertQuery = `INSERT INTO nfl.games_jakes_rankings (season, playerId, ${rank}, totalJakes)
                                          VALUES (${s}, ${rplId}, 1, 1)
                                          ON DUPLICATE KEY UPDATE ${rank} = ${rank}+1, totalJakes=totalJakes+1`;
                
                await queryDB(rankingInsertQuery, [], res);
              }
            }
        
            
          } // End jakes insert loop

          // eslint-disable-next-line
            console.log(`Completed jakes for season: ${s}`);
        } // End seasons of jakes
      } // End jakes if      
    } // End Season Loop                

    res.json({done: true, success: true });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.post('/add/team', async (req, res) => {
  try {
    var teamData = req.body;
    //console.log('req.body:', req.body);
    var queryData = getKeysAndValues(teamData);
    //console.log('querydata:', queryData);
    //return queryData;

    let query = ` INSERT INTO nfl.teams 
                    (${queryData.keysSQL}) 
                  VALUES (${queryData.valsSQL});`;

    await queryDB(query, queryData.params, res);  
    res.json({done: true, success: true });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.post('/add/player', async (req, res) => {
  try {
    var playerData = req.body;
    var queryData = getKeysAndValues(playerData);
    //return queryData;
    let query = ` INSERT INTO nfl.players 
                    (${queryData.keysSQL}) 
                  VALUES (${queryData.valsSQL});`;

    await queryDB(query, queryData.params, res);  
    res.json({done: true, success: true });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.post('/update/games', async (req, res) => {
  try {
    var teamData = req.body;
    var queryData = getKeysAndValues(teamData);

    
  } catch (err) {
    res.status(500).json(error);
  }
});

//

function getKeysAndValuesForUpdate(obj){
  //console.log(obj);
  //var keys = Object.keys(obj).join(',');
  //var vals = [];
  //var params = [];
  var keys = Object.keys(obj);
  var params = [];
  var updates = [];

  keys.forEach((k) => {
    let updateString = `${k} = ?`;
    updates.push(updateString);
    params.push(obj[k]);
  });

  return {
    sql: updates.join(","),
    params: params 
  };
}

function getKeysAndValues(obj){
  //console.log(obj);
  var keys = Object.keys(obj).join(',');
  var vals = [];
  var params = [];

  Object.keys(obj).forEach((v) => {
    vals.push('?');
    params.push(obj[v]);
  });

  var valSQL = vals.join(',');
  return {
    params: params,
    keysSQL: keys,
    valsSQL: valSQL
  };
}

module.exports = router;