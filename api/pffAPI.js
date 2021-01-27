const mariadb = require('mariadb');
const moment  = require('moment');
const express = require('express');
const router  = express.Router();
const pool    = mariadb.createPool(require('../config.json'));
const fetch = require("node-fetch");
const { weekdaysMin } = require('moment');
const { query } = require('express');
const { of } = require('core-js/fn/array');
var error   = { message: 'Error!', code: 0 };

Object.prototype.clone = Array.prototype.clone = function()
{
    if (Object.prototype.toString.call(this) === '[object Array]')
    {
        var clone = [];
        for (var i=0; i<this.length; i++)
            clone[i] = this[i].clone();

        return clone;
    } 
    else if (typeof(this)=="object")
    {
        var clone = {};
        for (var prop in this)
            if (this.hasOwnProperty(prop))
                clone[prop] = this[prop].clone();

        return clone;
    }
    else
        return this;
}

async function calculateESPNUltimate(player_stats, player, season, player_id) {
  var td = player.td;
  var sacks = player.sacks;
  var qbr = player.qbr;
  var jake = ((parseInt(player.int) + parseInt(player.fumbles)) * 1/6) * 100;
  var perfect = 1075;
  var birthday_score = 10000;
  var ultimate = 0;
  // Idea is that a perfect jake is 1075 + 10000 = 11075 (jan 10, 1975 - delhomme's bday!)
  // Gotta get some historical shit. 
  var search_name = player.split(' ')[0] + ' ' + player.split(' ')[1];
  var histResp = await fetch(`http://xperimental.io:4200/api/v1/get/pff/player_history_name/`, {
    method: 'post',              
    headers: {
      'Accept': 'application/json',
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ name: search_name  })
  });

  var histRespJSON = await histResp.json();
  var history = histRespJSON.history;

  // History data that we care about is jake position totals. 
  // There is also a 'gameCount' field that we can use to get totals
  // History should account for 20% of the score
  // Jakes total is slightly weighted. Heavy weight on first.
  if(!history) {
    history_score = 0.00;
  } else {
    var jp1 = history.jake_position_1 ? history.jake_position_1 : 0;
    var jp2 = history.jake_position_2 ? history.jake_position_2 : 0;
    var jp3 = history.jake_position_3 ? history.jake_position_3 : 0;
    var jp4 = history.jake_position_4 ? history.jake_position_4 : 0;

    var raw_jakes_total = jp1 + jp2 + jp3 + jp4;
    var jakes_total = ( 
      (jp1 * 0.65) + 
      (jp2 * 0.20) + 
      (jp3 * 0.10) + 
      (jp4 * 0.05)
    );

    var game_total = history.gameCount;
    var history_score = (jakes_total * (1 + (raw_jakes_total/game_total)))*10;
  }
  if(history_score > 200) history_score = 200.00;

  // Jake score makes up the majority.
  ultimate += jake * 5; // up to 500 (or more theoretically)
  
  // Sacks add 100 more. If 10 or more, 100
  ultimate += (sacks > 10 ? 100 : sacks * 10);

  // In this case, this will flip qbr upside down.
  // A 0.00 qbr (1.00) = 158.3 points, and a 158.3 qbr = 1 point;
  // It multiplies the result to get a value max of 300;
  if(!qbr) qbr = 0;
  if(qbr === 0) qbr = 1;
  var qbr_score = (((1/qbr)*158.3)*1.895);
  if(qbr_score > 300) qbr_score = 300.00
  ultimate += qbr_score    
  
  // TD's work like sacks in reverse, 0 td's = no subtraction, perfect is achievable.
  ultimate -= (td > 10 ? 100 : td * 10);

  // 1000 only gets us soooo far.
  ultimate += 75;

  var game_day = moment(player_stats.game_date).format('MM-DD');
  var bday = moment(player_stats.birthday, 'YYYY-MM-DD').format('MM-DD');

  if(bday === game_day) {
    ultimate += birthday_score;
  }

  if(ultimate > (perfect + birthday_score)) {
    ultimate = perfect + birthday_score;
  }

  // If jake is zero, then they are not eligible for the rating.
  if(jake === 0.00) ultimate = 0.00;
  return ultimate.toFixed(3);   
};

function getKeysAndValuesForUpdate(obj){
  //console.log(obj);
  //var keys = Object.keys(obj).join(',');
  //var vals = [];
  //var params = [];
  var keys = Object.keys(obj);
  var params = [];
  var updates = [];

  keys.forEach((k) => {
    if(k === 'id' || k === 'season' || k === 'week') {
      return;
    }
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

async function parseESPNGames(espnData, season, week) {
  var games = espnData;
  var parsedGames = [];
  for(var g=0;g<games.length;g++) {
    var game = games[g].competitions[0];
    var home_key = game.competitors[0].homeAway === 'home' ? 0 : 1;
    var away_key = home_key === 0 ? 1 : 0;
    var away_team = game.competitors[away_key].team; // id, location, name, abbreviation, displayName, shortDisplayName, color, alternateColor, isActive
    var home_team = game.competitors[home_key].team;
    
    var away_score = parseInt(game.competitors[away_key].score);
    var home_score = parseInt(game.competitors[home_key].score);

    var winner = away_score > home_score ? 'away' : 'home';
    if(away_score === home_score) winner = 'tie';  
    
    try{     
      if(away_team.abbreviation === 'WSH') away_team.abbreviation = 'WAS';
      if(home_team.abbreviation === 'WSH') home_team.abbreviation = 'WAS';
      if(away_team.abbreviation === 'LAR') away_team.abbreviation = 'LA';
      if(home_team.abbreviation === 'LAR') home_team.abbreviation = 'LA';
      if(away_team.abbreviation === 'CLE') away_team.abbreviation = 'CLV';
      if(home_team.abbreviation === 'CLE') home_team.abbreviation = 'CLV';
      if(away_team.abbreviation === 'BAL') away_team.abbreviation = 'BLT';
      if(home_team.abbreviation === 'BAL') home_team.abbreviation = 'BLT';
      if(away_team.abbreviation === 'HOU') away_team.abbreviation = 'HST';
      if(home_team.abbreviation === 'HOU') home_team.abbreviation = 'HST';
      if(away_team.abbreviation === 'ARI') away_team.abbreviation = 'ARZ';
      if(home_team.abbreviation === 'ARI') home_team.abbreviation = 'ARZ';

      var teamIdAR = await fetch(`http://xperimental.io:4200/api/v1/get/pff/team/${away_team.abbreviation}/${season}`);
      var teamIdAJ = await teamIdAR.json();       
      
      if(!teamIdAJ.team[0]) console.log(away_team);

      var awayteamId = teamIdAJ.team[0].franchise_id;

      var teamIdHR = await fetch(`http://xperimental.io:4200/api/v1/get/pff/team/${home_team.abbreviation}/${season}`);
      var teamIdHJ = await teamIdHR.json();

      if(!teamIdHJ.team[0]) console.log(home_team);
      var hometeamId = teamIdHJ.team[0].franchise_id;
      var game = {
        score_away: away_score,
        score_home: home_score,
        winner: winner,
        winner_id: (winner === 'tie' ? 0 : winner === 'away' ? awayteamId : hometeamId),
        loser_id: (winner === 'tie' ? 0 : winner === 'away' ? hometeamId : awayteamId),
        away_team_id: awayteamId,
        home_team_id: hometeamId,
        week: week,
        season: season, 
        espn_game_id: game.id
      };

      parsedGames.push(game);
    } catch(err) {
      console.log('error:', err);
      return err;
    }
  }

  return parsedGames;
}

async function parseESPNGamesFromSchedule(espnData, season, week) {
  var schedule = espnData.content.schedule;
  var parsedGames = [];
  schedule.forEach(date => {
    var games = date.games;
    parsedGames.concat(parseESPNGames(games, season, week));
  });

  return parsedGames;
}

async function getESPNPassingDataForGame(gameId, season, week) {
  // Time to update stats for players, up here now.
  // https://secure.espn.com/core/nfl/boxscore?gameId=401131037&xhr=1&render=false
  var espnGameDataReq = await fetch(`https://secure.espn.com/core/nfl/boxscore?gameId=${gameId}&xhr=1&render=false`);      
  var espnGameData = await espnGameDataReq.json();
  var gameStats = null;
  var gamePlayers = [];

  if(espnGameData) {
    if(espnGameData.gamepackageJSON) {
      gameStats = espnGameData.gamepackageJSON;
    }
  }

  if(gameStats) {
    gameStats.boxscore.players.forEach(player => {
      // Passing should always be in the 0th index.
      var teamFumblingAthletes = player.statistics[player.statistics.findIndex(statItem => statItem.name === 'fumbles')].athletes;
      var teamPassingAthletes = player.statistics[player.statistics.findIndex(statItem => statItem.name === 'passing')].athletes;
      var teamRushingAthletes = player.statistics[player.statistics.findIndex(statItem => statItem.name === 'rushing')].athletes;
      var playerTeamInfo = player.team;

      // Get passing only stats for now, we'll do fumbles later
      for(var a=0;a<teamPassingAthletes.length;a++) {
        var qb = teamPassingAthletes[a].athlete;
        var qbStats = teamPassingAthletes[a].athlete.stats;
        var qbCompAtt = qbStats[0].split('/');
        var qbID = qb.id;
        var qbFumbles = { fum: 0, lost: 0, rec: 0 };
        var qbPassing = null;      
        var qbRushing = { att: 0, yards: 0, ypa: 0.0, td: 0, long: 0 };

        // Check eligibility, we want at least 5 passes attempted to get a jake score
        // We have to account for less than 10 because of Tim fucking Tebow, who went 2/8 in a playoff game. Asshole.
        if(qbCompAtt[1] >= 5) {
          qbPassing = { 
            att: qbCompAtt[1], 
            comp: qbCompAtt[0], 
            comp_per: (qbCompAtt[1]/qbCompAtt[0]).toFixed(2), 
            yards: qbStats[1], 
            ypa: qbStats[2], 
            td: qbStats[3], 
            int: qbStats[4], 
            sacks: qbStats[5], 
            qbr: qbStats[6], 
            rtg: qbStats[7] 
          };
          // OK so we have passes, now we need to get fumbles if there are any
          var qbFumbler = teamFumblingAthletes.find(fumAthlete => fumAthlete.athlete.id === qbID);          
          if(qbFumbler.stats) qbFumbles = { fum: qbFumbler.stats[0], lost: qbFumbler.stats[1], rec: qbFumbler.stats[2] };

          var qbRusher = teamRushingAthletes.find(rushAthlete => rushAthlete.athlete.id === qbID);
          if(qbRusher.stats) qbRushing = { att: qbRusher.stats[0], yards: qbRusher.stats[1], ypa: qbRusher.stats[2], td: qbRusher.stats[3], long: qbRusher.stats[4] };

          var player = {
            id: 0,
            ints: qbPassing.int,
            fumbles: qbFumbles.lost,
            att: qbPassing.att,
            comp: qbPassing.comp,
            player: qb.displayName,
            player_id: qbID,
            rush_carries: qbRushing.att,
            rush_tds: qbRushing.td,
            rush_yds: qbRushing.yards,
            tds: qbPassing.td,
            sacks: qbPassing.sacks,
            team: playerTeamInfo.abbreviation,
            team_id: playerTeamInfo.id,
            season: season,
            week: week,
            qbr: qbPassing.qbr,
            ypa: qbPassing.ypa,
            comp_per: qbPassing.comp_per,
            yds: qbPassing.yards,
            jake_score: (parseInt(qbPassing.int) + parseInt(qbFumbles.lost)) * 1/6,
            ultimate_score: 0.00
          };    

          player.ultimate_score = await calculateESPNUltimate(player, player.player, season, player.player_id);
          gamePlayers.push(player);
        
        } 
      };        
    });
  }

  return gamePlayers;
}

function getCurrentWeek(calendar) {
  var currentDate = moment();
  var seasonStart = moment(calendar.startDate);
  var seasonEnd = moment(calendar.endDate);
  var currentWeek = 0;

  // Then we have data and need to update instead of insert.
  var l_games_resp = await fetch(`http://xperimental.io:4200/api/v1/get/pff/games/${season}/${week}`);
  var l_games_json = await l_games_resp.json();
  var l_games = l_games_json.games;
  var oldest_last_updated = null;
  var most_recent_last_updated = null;
  var force_update = false;
  
  // Get the last last_update
  l_games.forEach(game => {
    var glu = moment(game.last_updated);
    if(!most_recent_last_updated) most_recent_last_updated = glu;
    if(!oldest_last_updated) oldest_last_updated = glu;
    
    if(glu > most_recent_last_updated) most_recent_last_updated = glu;
    if(glu < oldest_last_updated) oldest_last_updated = glu;
  });

  if(currentDate > seasonEnd) {
    // We're in the post season, so the last week will always be the SB
    // SB = Week 22, 17 weeks for reg, 5 for post
    currentWeek = 22;
    if (most_recent_last_updated < seasonEnd) force_update = true;
  }

  if(currentDate < seasonStart) { 
    currentWeek = 1;
    if (most_recent_last_updated < seasonStart) force_update = false;
  }

  if(calendar.entries) {
    for(var e=0;e<calendar.entries.length;e++) {
      var entry = calendar.entries[e];

      if(most_recent_last_updated < entry.endDate) force_update = true;
      if(oldest_last_updated < entry.endDate) force_update = true;

      // Check if we are within the bounds of the possible weeks, if we find one, then we are returning the value (which is the week #)
      if(currentDate >= moment(entry.startDate) && currentDate <= moment(entry.endDate)) {
        currentWeek = parseInt(entry.value);
        force_update = true;
        break;
      }
    }
  }

  return { currentWeek: currentWeek, forceUpdate: force_update };
}

async function updateCurrentWeek(season, week = 0) {
  try {
    var locked = true;
    //var locked = await checkWeekLocked();
    //if(!locked) {}
    // We need to figure out a few things before we get started.
    // #1 - Figure out what the date is, and what the last game date WAS, we can use PFF for this
    // #2 - Grab the latest scores if the date of the last game matches today's date
    // #3 - After the last current week is complete, and the next has not started yet, go to cached data and do not update
    // #4 -       
    var seasonType = 0;    
    var adjustedWeek = 0;
    if(week >= 1 && week <= 17) seasonType = 1;
    if(week > 17) {
      seasonType = 2;
      adjustedWeek = week - 17;
    }  

    // Get the ESPN Scoreboard, which actually has all of the data we need...
    var espn_current_stats_r = await fetch(`http://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard`);      
    var espn_current_stats = await espn_current_stats_r.json();
    var currentWeekData = getCurrentWeek(espn_current_stats.leagues[0].calendar[seasonType]);
    var currentWeek = currentWeekData.currentWeek;
    var update = currentWeekData.forceUpdate;
    var espn_parsed_games = null;

    if (week !== currentWeek) {
      if(update) {
        // Then we are likely browsing history...
        // So we need to get the schedule for the selected week, not the current one
        var espnSchedule = await fetch(`https://secure.espn.com/core/nfl/schedule?year=${season}&week=${adjustedWeek}&seasontype=${seasonType+1}&xhr=1&render=false`);      
        var espnScheduleData = await espnSchedule.json();

        // This is slightly different in structure and needs some looping, so a new function was created.
        espn_parsed_games = await parseESPNGamesFromSchedule(espnScheduleData, season, week);
      } else {
        return {success: true, msg: 'update not necessary for this season + week'};
      }
    } else {
      // Fetch Current Week Stats from ESPN // Note! This is only THIS week, and you cannot set the week.
      espn_parsed_games = await parseESPNGames(espn_current_stats.events, season, week);
    }

    // Then update games
    for(var eg=0;eg<espn_parsed_games.length;eg++) {
      var local_game = false;
      var espn_game = espn_parsed_games[eg];

      // We just need to use this url now, as there is a key check that will auto-update if it exists
      var url = `http://xperimental.io:4200/api/v1/add/pff/game`;
      var updated_game_resp = await fetch(url, {
        method: 'post',              
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(insertGame)
      });

      var updatedGame = await updated_game_resp.json();            
    } 

    // espn_passers should now be the equivalent of the yahoo passers data we generated before.
    var espn_passers = await getESPNPassingDataForGame(espn_game.espn_game_id, season, week);    
    var l_players_resp = await fetch(`http://xperimental.io:4200/api/v1/get/pff/players/${season}/${week}`);
    var l_players_json = await l_players_resp.json();      
    var l_players = l_players_json.qbs;
    
    // OK, we need to update players and games, if they match - we don't care if they changed, just update anyway.
    // Player Update first.      

    for(var ep=0;ep<espn_passers.length;ep++) {
      var local_player = false;
      var espn_player_pass = espn_passers[ep];
      var url = '';
      var lpindex = l_players.findIndex((player) => {
        return player.player === espn_player_pass.player
      });

      if(lpindex >= 0) {
        local_player = l_players[lpindex];
      }

      if(local_player) {
        espn_player_pass.id = local_player.id;
        url = `http://xperimental.io:4200/api/v1/update/pff/week/`;
      } else {
        url = `http://xperimental.io:4200/api/v1/add/pff/week`;
      }

      var updated_resp = await fetch(url, {
        method: 'post',              
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(espn_player_pass)
      });

      var updatedPlayer = await updated_resp.json();        
    }  

    if(moment().day() > 1 && moment().day() < 5 && !locked) {
      // 
      //await setJakePositions();
      //await calculateHistoricalJakes();
      //await calcUlts();
      // await weekUpdater();
    }
    
  
    return {success: true, msg: 'probably updated...'};
  } catch (err) {
    //eslint-disable-next-line
    console.log('error:', err);
    return err;
  }
}

async function queryDB(query, params){
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
  } finally {
    if (conn) conn.end();
  }
}

router.post('/add/pff/player/', async (req, res) => {
  try {
    var playerData = req.body;
    var insertPFFData = getKeysAndValues(playerData);
    let insertPFFQuery = `INSERT INTO nfl.pff_players
                            (${insertPFFData.keysSQL}) 
                          VALUES (${insertPFFData.valsSQL});`;

    var inserted = await queryDB(insertPFFQuery, insertPFFData.params);  
    res.json({done: true, success: inserted.success, id: inserted.id });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.post('/add/pff/game', async (req, res) => {
  try {
    var weekData = req.body;
    var insertPFFData = getKeysAndValues(weekData);
    let insertPFFQuery = `INSERT INTO nfl.pff_games
                            (${insertPFFData.keysSQL}) 
                          VALUES (${insertPFFData.valsSQL})
                          ON DUPLICATE KEY UPDATE score_away = ${weekData.score_away}, score_home = ${weekData.score_home};`;

    await queryDB(insertPFFQuery, insertPFFData.params);  
    res.json({done: true, success: true });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.post('/add/pff/team', async (req, res) => {
  try {
    var weekData = req.body;
    var insertPFFData = getKeysAndValues(weekData);
    let insertPFFQuery = `INSERT INTO nfl.pff_teams
                            (${insertPFFData.keysSQL}) 
                          VALUES (${insertPFFData.valsSQL});`;

    await queryDB(insertPFFQuery, insertPFFData.params);  
    res.json({done: true, success: true });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.post('/add/pff/week', async (req, res) => {
  try {
    var weekData = req.body;
    var insertPFFData = getKeysAndValues(weekData);
    let insertPFFQuery = `INSERT INTO nfl.pff_qb_stats
                            (${insertPFFData.keysSQL}) 
                          VALUES (${insertPFFData.valsSQL});`;

    await queryDB(insertPFFQuery, insertPFFData.params);  
    res.json({done: true, success: true });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.get('/get/pff/games/:season/:week', async (req, res) => {
  try {
    var gamesQ = `SELECT g.*,
                    (SELECT t.abbreviation FROM nfl.pff_teams t WHERE t.franchise_id = g.away_team_id and t.season = g.season) as away_team,
                    (SELECT t.abbreviation FROM nfl.pff_teams t WHERE t.franchise_id = g.home_team_id and t.season = g.season) as home_team,                    
                  FROM nfl.pff_games g
                  WHERE g.season = ${req.params.season} AND g.week = ${req.params.week}`;
    var games = await queryDB(gamesQ, []);
    res.json({done: true, success: true, games: games });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.get('/get/pff/player_history/:id', async (req,res) => {
  try {    
    console.log(req.params.id);
    var historyquery = `SELECT h.id, h.pff_id, h.jake_position_1, h.jake_position_2, h.jake_position_3, h.jake_position_4,
                          h.ult_jake_position_1, h.ult_jake_position_2, h.ult_jake_position_3, h.ult_jake_position_4, 
                          h.record_jake, h.record_ultimate, 
                          (SELECT COUNT(s.id) as cnt FROM nfl.pff_qb_stats s WHERE s.player_id = h.pff_id) as gameCount
                        FROM nfl.pff_jakes_history h 
                        WHERE h.pff_id = ${req.params.id}`;
    var history_data = await queryDB(historyquery, []);
    res.json({done: true, success: true, history: history_data[0] });
  
  } catch (err) {
    res.status(500).json(error);
  }
});

router.post('/get/pff/player_history_name/', async (req,res) => {
  try {    
    var playerData = req.body;
    var name_string = '%' + playerData.name +  '%';
    var historyquery = `SELECT h.id, h.pff_id, h.jake_position_1, h.jake_position_2, h.jake_position_3, h.jake_position_4,
                          h.ult_jake_position_1, h.ult_jake_position_2, h.ult_jake_position_3, h.ult_jake_position_4, 
                          h.record_jake, h.record_ultimate, 
                          (SELECT COUNT(s.id) as cnt FROM nfl.pff_qb_stats s WHERE s.player_id = h.pff_id) as gameCount
                        FROM nfl.pff_players p 
                          JOIN nfl.pff_jakes_history h ON h.pff_id = p.pff_id
                        WHERE p.full_name LIKE ?`;
    var history_data = await queryDB(historyquery, [name_string]);
    res.json({done: true, success: true, history: history_data[0] });
  
  } catch (err) {
    res.status(500).json(error);
  }
});

router.get('/get/pff/player_details/', async (req, res) => {
  try {
    
    var qbsquery = `SELECT DISTINCT p.pff_id as player_id, p.full_name FROM nfl.pff_players p`;
    var qbs = await queryDB(qbsquery, []);
    res.json({done: true, success: true, qbs: qbs });
  
  } catch (err) {
    res.status(500).json(error);
  }
});

router.get('/get/pff/player_list_ids/', async (req, res) => {
  try {
    
    var qbsquery = `SELECT DISTINCT p.player_id, p.player FROM nfl.pff_qb_stats p`;
    var qbs = await queryDB(qbsquery, []);
    res.json({done: true, success: true, qbs: qbs });
  
  } catch (err) {
    res.status(500).json(error);
  }
});

router.get('/get/pff/player_list/:season/', async (req, res) => {
  try {
    if (req.params.season) {
      var qbsquery = `SELECT DISTINCT p.player FROM nfl.pff_qb_stats p WHERE p.season = ${req.params.season} GROUP BY player`;
      var qbs = await queryDB(qbsquery, []);
      res.json({done: true, success: true, qbs: qbs });
    } else {
      var qbsquery = `SELECT DISTINCT p.player_id, p.player FROM pff_qb_stats p`;
      var qbs = await queryDB(qbsquery, []);
      res.json({done: true, success: true, qbs: qbs });
    }
  } catch (err) {
    res.status(500).json(error);
  }
});

router.get('/get/pff/players/:season/:week', async (req, res) => {
  try {
    var qbsquery = `SELECT p.* FROM nfl.pff_qb_stats p WHERE p.season = ${req.params.season} AND p.week = ${req.params.week}`;
    var qbs = await queryDB(qbsquery, []);
    res.json({done: true, success: true, qbs: qbs });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.get('/get/pff/stats/', async (req, res) => {
  try {
    var qbsquery = `  SELECT p.*, pl.birthday, g.game_date
                      FROM nfl.pff_qb_stats p 
                        JOIN nfl.pff_players pl ON p.player_id = pl.pff_id 
                        JOIN nfl.pff_games g ON g.season = p.season and g.week = p.week and (g.away_team_id = p.team_id OR g.home_team_id = p.team_id)
                      ORDER BY p.season, p.week`;
    var qbs = await queryDB(qbsquery, []);
    res.json({done: true, success: true, qbs: qbs });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.post('/get/player/name/', async (req, res) => {
  try {
    var playerData = req.body;
    var name_string = '%' + playerData.name +  '%';
    if(playerData.season && playerData.week) {
      var playerq = ` SELECT p.*, g.game_date
                      FROM nfl.pff_players p 
                        JOIN nfl.pff_qb_stats q ON q.player_id = p.pff_id
                        JOIN nfl.pff_games g ON (q.team_id = g.away_team_id OR q.team_id = g.home_team_id)                   
                      WHERE p.full_name LIKE ? AND q.season = ? and q.week = ?`;
      var player = await queryDB(playerq, [name_string, playerData.season, playerData.week]);
    } else {
      var playerq = ` SELECT p.* 
                      FROM nfl.pff_players p 
                      WHERE p.full_name LIKE ?`;
      var player = await queryDB(playerq, [name_string]);                      
    }
    
    
    res.json({done: true, success: true, player: player[0] });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.get('/get/jakes/:season/:week', async (req, res) => {
  try {
    let weekQuery = '';
    let seasonQuery = '';
    let week = parseInt(req.params.week);
    let season = parseInt(req.params.season);
    let orderByAdd = 'p.jake_score DESC, p.ultimate_score DESC';
    if(week > 0) {
      weekQuery = `and p.week = ${week}`;
    } else {
      orderByAdd = 'p.week, p.jake_score DESC, p.ultimate_score DESC';
    }

    if(season !== 0 && week !== 0) {
      seasonQuery = `p.season = ${season}`      
    } else {
      orderByAdd = 'p.season, p.week, p.jake_score DESC, p.ultimate_score DESC';
    }

    //console.log('reqp', req.params);

    var jakesQ = `SELECT  p.id, p.player, p.player_id, p.att, p.comp, p.fumbles, p.ints, p.rush_carries, p.rush_tds,
                          p.rush_yds, p.tds, p.yds, p.sacks, p.qbr, p.ypa,
                          ROUND(p.jake_score * 100, 2) as jake_score, p.jake_position,
                          ROUND((p.comp / p.att) * 100, 2) as comp_per,  
                          (p.tds + p.rush_tds) as total_tds,                        
                          g.score_away, g.score_home, t.abbreviation, CONCAT(t.city, ' ', t.nickname) as teamName, t.primary_color, t.secondary_color, p.season, p.week,
                          IF(g.score_away > g.score_home, CONCAT('Final: ', g.score_away, '-', g.score_home), CONCAT('Final: ', g.score_home, '-', g.score_away)) as finalScore, p.ultimate_score,
                          pl.birthday, g.id as game_id, g.game_date
                  FROM nfl.pff_qb_stats p
                    JOIN nfl.pff_games g ON p.team_id = g.loser_id and p.season = g.season and p.week = g.week
                    JOIN nfl.pff_teams t ON p.team = t.abbreviation and p.season = t.season
                    JOIN nfl.pff_players pl ON pl.pff_id = p.player_id
                  WHERE ${seasonQuery} ${weekQuery} AND p.jake_score > 0
                  ORDER BY ${orderByAdd} `;
                  //console.log('reqp', jakesQ);

    var jakes = await queryDB(jakesQ, []);
    res.json({done: true, success: true, jakes: jakes });
  } catch (err) {
    res.status(500).json(err);
  }
});

router.get('/get/player_stats/:season/:week', async (req, res) => {
  try {
    let weekQuery = '';
    let seasonQuery = '';
    let week = parseInt(req.params.week);
    let season = parseInt(req.params.season);
    let orderByAdd = 'teamName';
    if(week > 0) {
      weekQuery = `and p.week = ${week}`;
    } 

    if(season !== 0 && week !== 0) {
      seasonQuery = `p.season = ${season}`      
    } 
    console.log('reqp', req.params);

    var playersQ = `SELECT  p.id, pl.pff_id, p.player, p.player_id, p.att, p.comp, p.fumbles, p.ints, p.rush_carries, p.rush_tds,
                          p.rush_yds, p.tds, p.yds, p.sacks, p.qbr, p.ypa,
                          ROUND(p.jake_score * 100, 2) as jake_score, p.jake_position,
                          ROUND((p.comp / p.att) * 100, 2) as comp_per,  
                          (p.tds + p.rush_tds) as total_tds,                        
                          g.score_away, g.score_home, t.abbreviation, CONCAT(t.city, ' ', t.nickname) as teamName, t.primary_color, t.secondary_color, p.season, p.week,
                          IF(g.score_away > g.score_home, CONCAT('Final: ', g.score_away, '-', g.score_home), CONCAT('Final: ', g.score_home, '-', g.score_away)) as finalScore, p.ultimate_score,
                          pl.birthday, g.id as game_id, g.game_date
                  FROM nfl.pff_qb_stats p
                    JOIN nfl.pff_games g ON (p.team_id = g.loser_id OR p.team_id = g.winner_id) and p.season = g.season and p.week = g.week
                    JOIN nfl.pff_teams t ON p.team = t.abbreviation and p.season = t.season
                    JOIN nfl.pff_players pl ON pl.pff_id = p.player_id
                  WHERE ${seasonQuery} ${weekQuery}
                  ORDER BY ${orderByAdd} `;
                 // console.log('reqp', playersQ);

    var players = await queryDB(playersQ, []);

    players.forEach(async (player, playerIndex) => {
      var historyquery = `SELECT h.id, h.pff_id, h.jake_position_1, h.jake_position_2, h.jake_position_3, h.jake_position_4,
                          h.ult_jake_position_1, h.ult_jake_position_2, h.ult_jake_position_3, h.ult_jake_position_4, 
                          h.record_jake, h.record_ultimate, 
                          (SELECT COUNT(s.id) as cnt FROM nfl.pff_qb_stats s WHERE s.player_id = h.pff_id) as gameCount
                        FROM nfl.pff_jakes_history h 
                        WHERE h.pff_id = ${player.pff_id}`;
      var history_data = await queryDB(historyquery, []);
      player.history = history_data;
      players[playerIndex] = player;
    }); 

    res.json({done: true, success: true, players: players });
  } catch (err) {
    res.status(500).json(err);
  }
});


router.get('/get/jakes/def/', async (req, res) => {
  try {
    var jakes_team_query = `SELECT s.season, s.week, s.player, s.player_id, s.team_id, s.team, s.jake_score, s.jake_position, g.winner_id,
                              (SELECT t1.nickname FROM nfl.pff_teams t1 WHERE t1.franchise_id = g.winner_id and t1.season = g.season) as winner,
                              (SELECT t1.color_metadata FROM nfl.pff_teams t1 WHERE t1.franchise_id = g.winner_id and t1.season = g.season) as winner_color,
                              (SELECT t2.nickname FROM nfl.pff_teams t2 WHERE t2.franchise_id = g.loser_id and t2.season = g.season) as loser,
                              (SELECT t2.color_metadata FROM nfl.pff_teams t2 WHERE t2.franchise_id = g.winner_id and t2.season = g.season) as loser_color
                            FROM nfl.pff_qb_stats s
                              JOIN nfl.pff_games g ON s.game_id = g.id and s.team_id = g.loser_id and g.season = s.season and g.week = s.week
                            WHERE s.season > 2007 AND s.jake_score > 0 and s.jake_position = 1
                            ORDER BY winner`;

    var jakes_team = await queryDB(jakes_team_query, []);
    res.json({done: true, success: true, history: jakes_team });
  } catch (err) {
    res.status(500).json(err);
  }
});

router.get('/get/jakes_history/', async (req, res) => {
  try {
    var jakesQ = `SELECT p.full_name, 
                    (h.jake_position_1 + h.jake_position_2 + h.jake_position_3) as totalJakes, 
                    h.jake_position_1, h.jake_position_2, h.jake_position_3, h.jake_position_4, 
                    h.record_jake, (SELECT count(s.id) FROM nfl.pff_qb_stats s WHERE s.player_id = h.pff_id) as totalGames
                  FROM nfl.pff_jakes_history h
                    JOIN nfl.pff_players p ON p.pff_id = h.pff_id
                  ORDER BY h.jake_position_1 DESC, totalJakes DESC
                  LIMIT 10`;

    var jakes = await queryDB(jakesQ, []);

    res.json({done: true, success: true, jakes: jakes });
  } catch (err) {
    res.status(500).json(err);
  }
});

router.get('/get/pff/team/:team_abbr/:season', async (req, res) => {
  try {
    var teamq = `SELECT t.* FROM nfl.pff_teams t WHERE t.abbreviation = '${req.params.team_abbr}' and t.season = ${req.params.season}`;
    var team = await queryDB(teamq, []);
    res.json({done: true, success: true, team: team });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.get('/get/pff/teams/:season', async (req, res) => {
  try {
    var teamq = `SELECT t.* FROM nfl.pff_teams t WHERE t.season = ${req.params.season}`;
    var teams = await queryDB(teamq, []);
    res.json({done: true, success: true, teams: teams });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.get('/get/pff/teamname/:team_name/:season', async (req, res) => {
  try {
    var teamq = `SELECT t.* FROM nfl.pff_teams t WHERE t.nickname = '${req.params.team_name}' and t.season = ${req.params.season}`;
    var team = await queryDB(teamq, []);
    res.json({done: true, success: true, team: team });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.get('/pff/get/currentweek/:season', async (req, res) => {
  try {
    var week = req.params.week;
    var wquery = ` SELECT MAX(week) as mw FROM nfl.pff_qb_stats WHERE season = ${req.params.season}`;

    var weekinfo = await queryDB(wquery, []);
    res.json({done: true, success: true, week: weekinfo[0].mw });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.get('/update/currentweek/:season/:week', async (req, res) => {
  try {
    var updated = await updateCurrentWeek(req.params.season, req.params.week);
    res.json(updated);
  } catch (err) {
    res.status(500).json(error);
  }
});

router.post('/update/jakes', async (req, res) => {
  try {
    var scoreData = req.body;
    var scoreQuery = `UPDATE nfl.pff_games SET score_away = ${scoreData.score_away}, score_home = ${scoreData.score_home}
                       WHERE away_team_id = ${scoreData.away_team_id} AND home_team_id = ${scoreData.home_team_id} AND season = ${scoreData.season} AND week = ${scoreData.week}`;
    var scoreOK = await queryDB(scoreQuery, []);

    res.json({done: true, success: scoreOK });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.post('/update/jake_history/', async (req, res) => {
  try {
    var historyData = req.body;    
    var historyQ = `SELECT * FROM nfl.pff_jakes_history WHERE pff_id = ${historyData.pff_id}`;
    var history = await queryDB(historyQ, []);

    if(history.length > 0) {
      var updateData = getKeysAndValuesForUpdate(historyData);
      var histUpdQ = `UPDATE nfl.pff_jakes_history 
                        SET ${updateData.sql} 
                      WHERE pff_id = ${historyData.pff_id}`;
      var histOK = await queryDB(histUpdQ, updateData.params);
    } else {
      var insertHistData = getKeysAndValues(historyData);
      let histInsQ = `INSERT INTO nfl.pff_jakes_history
                        (${insertHistData.keysSQL}) 
                      VALUES (${insertHistData.valsSQL});`;
      var histOK = await queryDB(histInsQ, insertHistData.params);           
    }

    res.json({done: true, success: histOK ? 'yes' : 'no' });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.post('/update/pff/week/', async (req, res) => {
  try {
    var playerData = req.body;
    var updateData = getKeysAndValuesForUpdate(playerData);
    var play_upd = `  UPDATE nfl.pff_qb_stats SET ${updateData.sql} 
                      WHERE id = ${playerData.id} AND season = ${playerData.season} AND week = ${playerData.week}`;
    var playerOK = await queryDB(play_upd, updateData.params);

    res.json({done: true, success: playerOK });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.post('/update/pff/game/date', async (req, res) => {
  try {
    var scoreData = req.body;
    var scoreQuery = `UPDATE nfl.pff_games SET game_date = '${scoreData.game_date}', espn_game_id = ${scoreData.espn_game_id}
                       WHERE away_team_id = ${scoreData.away_team_id} AND home_team_id = ${scoreData.home_team_id} AND season = ${scoreData.season} AND week = ${scoreData.week}`;
    var scoreOK = await queryDB(scoreQuery, []);
    res.json({done: true, success: scoreOK });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.post('/update/pff/game/score', async (req, res) => {
  try {

    var scoreData = req.body;
    var pffQ = '';
    if(scoreData.stadium_id && scoreData.pff_id) pffQ = `,stadium_id = ${scoreData.stadium_id}, pff_id =${scoreData.pff_id}`;
    var scoreQuery = `UPDATE nfl.pff_games SET winner_id = ${scoreData.winner_id}, loser_id = ${scoreData.loser_id}, winner = '${scoreData.winner}', score_away = ${scoreData.score_away}, score_home = ${scoreData.score_home} ${pffQ} 
                       WHERE away_team_id = ${scoreData.away_team_id} AND home_team_id = ${scoreData.home_team_id} AND season = ${scoreData.season} AND week = ${scoreData.week}`;
    var scoreOK = await queryDB(scoreQuery, []);
    res.json({done: true, success: scoreOK });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.post('/update/pff/jake_pos/', async (req, res) => {
  try {
    var playerData = req.body;
    var play_upd = `  UPDATE nfl.pff_qb_stats SET jake_position = ${playerData.jake_position} 
                      WHERE id = ${playerData.id}`;
    var playerOK = await queryDB(play_upd, []);

    res.json({done: true, success: playerOK });
  } catch (err) {
    res.status(500).json(error);
  }
});

router.post('/update/pff/ultimate/', async (req, res) => {
  try {
    var playerData = req.body;
    var play_upd = `  UPDATE nfl.pff_qb_stats SET ultimate_score = ${playerData.ultimate} 
                      WHERE id = ${playerData.id}`;
    var playerOK = await queryDB(play_upd, []);

    res.json({done: true, success: playerOK });
  } catch (err) {
    res.status(500).json(error);
  }
});

module.exports = router; 