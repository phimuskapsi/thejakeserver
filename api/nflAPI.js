const { stat } = require('fs');
//import axios from 'axios';
const moment  = require('moment');
const fetch = require("node-fetch");
const { URL, URLSearchParams } = require('url');

class NFLAPI {
  constructor() {
    this.currentYear = parseInt(moment().format("YYYY"));
    this.feedBaseURL = 'https://feeds.nfl.com/feeds-rs/';
    this.schedules = {};
    this.preSeasons = {};
    this.regSeasons = {};
    this.postSeasons = {};
    this.players = [];
    this.teams = [];
    this.games = [];
    this.season = 2020;
  }

  init() {
    // eslint-disable-next-line
    //console.log('Filling DB for 2011 ----> 2012 seasons. Hold on to your butts!');

      //this.addDatesForGames().then((result) => {
      //  console.log('Result: ', result);
      //});
    //this.fixGames().then((fres) => {
      //console.log('Result: ', fres);    
      this.setJakePositions().then((res) => {
        console.log('Result: ', res);    
        this.calculateHistoricalJakes().then((result) => {
          console.log('Result: ', result);
          this.calcUlts().then((result2) => {
            console.log('Result: ', result2);
          });      
        });
      });
    //});
    
    // eslint-disable-next-line
    //console.log('Fixing Games...');
  }

  async calcDefStats() {
    try {
      var teamsResp = await fetch(`http://xperimental.io:4200/api/v1/get/jakes/def/`);
      var teamsRespJSON = await teamsResp.json();
      var history_teams = teamsRespJSON.history;

      var team_totals = {};
      var team_season_totals = {};
      var team_jakes_totals = {};
      
      for(var t=0;t<history_teams.length;t++) {
        var team = history_teams[t];
        console.log('starting team: ' + team.winner);
      
        if(!team_totals[team.winner_id]) team_totals[team.winner_id] = { total: 0, name: team.winner };

        if(!team_jakes_totals[team.player_id]) team_jakes_totals[team.player_id] = {};
        if(!team_jakes_totals[team.player_id][team.winner_id]) team_jakes_totals[team.player_id][team.winner_id] = { total: 0, team: team.loser, name: team.player, winner: team.winner };

        if(!team_season_totals[team.season]) team_season_totals[team.season] = {};
        if(!team_season_totals[team.season][team.winner_id]) team_season_totals[team.season][team.winner_id] = { total: 0, name: team.winner };


        team_totals[team.winner_id].total++;
        team_season_totals[team.season][team.winner_id].total++;
        team_jakes_totals[team.player_id][team.winner_id].total++;
      }      

    } catch (err) {
      res.status(500).json(err);
    }
  }

  async fixGames() {
    try {
      for(var s=2008;s<=2020;s++) {       
        console.log(`Season ${s} is starting....`); 
        for(var w=1;w<=17;w++) {
          if(s===2020 && w > 3) continue;
          console.log(`Season ${s} - Week ${w} is starting....`);          

          var l_players_resp = await fetch(`http://xperimental.io:4200/api/v1/get/pff/players/${s}/${w}`);
          var l_players_json = await l_players_resp.json();      
          var l_players = l_players_json.qbs;
    
          var yahoo_current_pstats_r = await fetch(`https://graphite-secure.sports.yahoo.com/v1/query/shangrila/weeklyStatsFootballPassing?season=${s}&league=nfl&sortStatId=PASSING_INTERCEPTIONS&week=${w}&count=200`);
          var yahoo_current_pstats = await yahoo_current_pstats_r.json();
          var yahoo_players = yahoo_current_pstats.data.leagues[0].leagueWeeks[0].leaders;

          for(var pfp=0;pfp<yahoo_players.length;pfp++) {
            var yahoo_player_pass = yahoo_players[pfp].player;
            var search_name = yahoo_player_pass.displayName.split(' ')[0] + ' ' + yahoo_player_pass.displayName.split(' ')[1];
            var player_name_resp = await fetch(`http://xperimental.io:4200/api/v1/get/player/name/`, {
              method: 'post',              
              headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json'
              },
              body: JSON.stringify({ name: search_name  })
            });

            var player_name_json = await player_name_resp.json();
            if(!player_name_json || !player_name_json.player) continue;
            var player_id = player_name_json.player.pff_id;           
            var yahoo_player_stats_pre = yahoo_players[pfp].stats
            var player_stats = {};
            yahoo_player_stats_pre.forEach((stat) => {
              player_stats[stat.statId] = stat.value;
            });

            var lpindex = l_players.findIndex((player) => {
              return player_name_json.player.pff_id === player.player_id
            });

            var local_player = false;
            if(lpindex > -1) {
              local_player = l_players[lpindex];              
            } else {
              continue;
            }

            var teamId = 0;
            var teamIdR = await fetch(`http://xperimental.io:4200/api/v1/get/pff/team/${local_player.team}/${s}`);
            var teamIdJ = await teamIdR.json();

            if(!teamIdJ.team[0]) {
              debugger;
              console.log('teamidj', teamIdJ);
              console.log('lp', local_player);
            }
            teamId = teamIdJ.team[0].franchise_id;              

            var statsPlayer = {
              id: local_player.id,
              ints: (player_stats.PASSING_INTERCEPTIONS ? parseInt(player_stats.PASSING_INTERCEPTIONS) : 0),
              fumbles: (player_stats.FUMBLES_LOST ? parseInt(player_stats.FUMBLES_LOST) : 0),
              att: (player_stats.PASSING_ATTEMPTS ? parseInt(player_stats.PASSING_ATTEMPTS) : 0),
              comp: (player_stats.PASSING_COMPLETIONS ? parseInt(player_stats.PASSING_COMPLETIONS) : 0),
              player: player_name_json.player.full_name,
              player_id: local_player.player_id,
              rush_carries: 0,
              rush_tds: 0,
              rush_yds: 0,
              tds: (player_stats.PASSING_TOUCHDOWNS ? parseInt(player_stats.PASSING_TOUCHDOWNS) : 0),
              sacks: (player_stats.SACKS_TAKEN ? parseInt(player_stats.SACKS_TAKEN) : 0),
              team: local_player.team,
              team_id: teamId,
              season: s,
              week: w,
              qbr: (player_stats.QB_RATING ? parseFloat(player_stats.QB_RATING) : 0),
              ypa: (player_stats.PASSING_YARDS_PER_ATTEMPT ? parseFloat(player_stats.PASSING_YARDS_PER_ATTEMPT) : 0),
              comp_per: (player_stats.COMPLETION_PERCENTAGE ? parseFloat(player_stats.COMPLETION_PERCENTAGE) : 0),
              yds: (player_stats.PASSING_YARDS ? parseInt(player_stats.PASSING_YARDS) : 0),
              jake_score: (parseInt(player_stats.PASSING_INTERCEPTIONS) + parseInt(player_stats.FUMBLES_LOST)) * 1/6,
              ultimate_score: 0.00
            };
      
            var updated_resp = await fetch(`http://xperimental.io:4200/api/v1/update/pff/week/`, {
              method: 'post',              
              headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json'
              },
              body: JSON.stringify(statsPlayer)
            });
      
            var updatedPlayer = await updated_resp.json();      
            
          }

          console.log(`Season ${s} - Week ${w} is complete.`);
        }
        console.log(`Season ${s} is complete.`);
      }

      return 'Completed update.';
    } catch (err) {
      console.log('error:', err);
      return false;
    }
  }

  async setJakePositions() {
    try {
      for(var s=2008;s<=2020;s++) {
        console.log('starting season: ' + s.toString());
        for(var w=1;w<=21;w++) {
          console.log('starting week: ' + w.toString());
          if(s === 2020 && w > 3) continue;
          var tempPlayersResp = await fetch(`http://xperimental.io:4200/api/v1/get/jakes/${s}/${w}`);
          var tempPlayersJSON = await tempPlayersResp.json();
          var tempPlayers = tempPlayersJSON.jakes;
          var records = 0;

          // Players will be all sorted by jake order
          // It will also sort by ultimate for ties
          // By week
          var pos = 0;
          var last_score = 0.00;
          for(var tp=0;tp<tempPlayers.length;tp++) {
            var player = tempPlayers[tp];
            if(player.jake_score === 0) {
              pos = 0;
            } else {
              if(last_score !== player.jake_score) {
                last_score = player.jake_score
                pos++;
              }
            }
            

            var update_pff_data = {
              id: player.id,
              jake_position: pos
            };

            var update_pff_idr = await fetch(`http://xperimental.io:4200/api/v1/update/pff/jake_pos/`, {
              method: 'post',              
              headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json'
              },
              body: JSON.stringify(update_pff_data)
            });

            var update_pff_idj = await update_pff_idr.json();
            //console.log('pff resp:', update_pff_idj);
            records++;
            if(!update_pff_idj.success) {
              console.log('failed, last data:', update_pff_data);
            }
          }
          
        }
      }

      return 'jakes positioned and complete. #records processed: ' + records.toString();
    } catch (err) {
      console.log('error:', err);
      return err;
    }
  }

  async calculateHistoricalJakes() {
    try {
      var records = 0;
      var players = {};
      for(var s=2008;s<=2020;s++) {
        console.log('starting season: ' + s.toString());
        
        var tempPlayersResp = await fetch(`http://xperimental.io:4200/api/v1/get/jakes/${s}/0`);
        var tempPlayersJSON = await tempPlayersResp.json();
        var tempPlayers = tempPlayersJSON.jakes;
        
        for(var t=0;t<tempPlayers.length;t++) {
          var player = tempPlayers[t];
          var historical_record = {
            pff_id: 0,
            jake_position_1: 0,
            jake_position_2: 0,
            jake_position_3: 0,
            jake_position_4: 0,
            record_jake: 0.00
          };

          if(!players[player.player_id]) {
            historical_record.pff_id = player.player_id;
            players[player.player_id] = historical_record;
          }

          if(player.jake_score > 0) {
            if(player.jake_score > players[player.player_id].record_jake) players[player.player_id].record_jake = player.jake_score;        
            switch(player.jake_position) {
              case 1: 
                players[player.player_id].jake_position_1++;
              break;

              case 2:
                players[player.player_id].jake_position_2++;
              break;

              case 3:
                players[player.player_id].jake_position_3++;
              break;

              case 4:
              default:
                players[player.player_id].jake_position_4++;
              break;
            }
          }
        }
      }

      for(var player_id in players) {
        var historicalPlayer = players[player_id];
        var update_pff_idr = await fetch(`http://xperimental.io:4200/api/v1/update/jake_history/`, {
          method: 'post',              
          headers: {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
          },
          body: JSON.stringify(historicalPlayer)
        });

        var update_pff_idj = await update_pff_idr.json();           
        //console.log(`historical player: ${player_id} OK: ${update_pff_idj.success}`);
      } 
    } catch (err) {
      console.log('error:', err);
      return err;
    }
  }

  async calculateUltimate(player_stats, birthdate) {
    var comp_per = player_stats.comp_per;
    var yards = player_stats.yds;
    var att = player_stats.att;
    var comp = player_stats.comp;
    var td = player_stats.tds;
    var int = player_stats.ints;
    var sacks = player_stats.sacks;
    var fumbles = player_stats.fumbles;
    var qbr = player_stats.qbr;
    var jake = parseFloat(((parseInt(player_stats.ints) + parseInt(player_stats.fumbles)) * 1/6) * 100);
    var perfect = 1075;
    var birthday_score = 10000;
    var ultimate = 0;
    //var history = {};

    // Idea is that a perfect jake is 1075 + 10000 = 11075 (jan 10, 1975 - delhomme's bday!)
    // Gotta get some historical shit. 
    var histResp = await fetch(`http://xperimental.io:4200/api/v1/get/pff/player_history/${player_stats.player_id}`);
    var histRespJSON = await histResp.json();
    var history = histRespJSON.history;

    //console.log('history', history);
    //return;

    // History data that we care about is jake position totals. 
    // There is also a 'gameCount' field that we can use to get totals
    // History should account for 20% of the score
    // Jakes total is slightly weighted. Heavy weight on first.
    var jp1 = history.jake_position_1 ? history.jake_position_1 : 0;
    var jp2 = history.jake_position_3 ? history.jake_position_2 : 0;
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
    if(history_score > 200) history_score = 200.00;

    // Jake score makes up the majority.
    ultimate += jake * 5; // up to 500 (or more theoretically)
      
    // Sacks add 100 more. If 10 or more, 100
    if(!sacks) sacks = 0;
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

    // Only add the bday penalty if the jake score is greater than 0
    if(bday === game_day && jake > 0.00) {
      ultimate += birthday_score;
    }

    if(ultimate > (perfect + birthday_score)) {
      ultimate = perfect + birthday_score;
    }
    
    // If jake is zero, then they are not eligible for the rating.
    if(jake === 0.00) ultimate = 0.00;
    return ultimate.toFixed(3);  
  };

  async calcUlts() {
    try {
      for(var s=2008;s<=2020;s++) {
        console.log(`starting season #${s}.`);
        var tempPlayersResp = await fetch(`http://xperimental.io:4200/api/v1/get/jakes/${s}/0`);
        var tempPlayersJSON = await tempPlayersResp.json();
        var tempPlayers = tempPlayersJSON.jakes;
        
        var records = 0;

        //console.log('tp:', tempPlayers);
        //return;

        for(var tp=0;tp<tempPlayers.length;tp++) {
          var player = tempPlayers[tp];
          var ultimate = await this.calculateUltimate(player, player.birthday);
          if(ultimate) {
            var pff_player_insert_r = await fetch(`http://xperimental.io:4200/api/v1/update/pff/ultimate/`, {
              method: 'post',              
              headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json'
              },
              body: JSON.stringify({id: player.id, ultimate: ultimate})
            });
            var pff_player_insert = await pff_player_insert_r.json();
            if(pff_player_insert) {
              //console.log('added: ', player.id);
            }
          } else {
            console.log('ultimate', ultimate);
            throw 'failed to get ultimate score';
          }

          var pi = tp + 1;
          records++;
          console.log(`player #${pi} calculated: ${ultimate}`)
        }
      }

      return 'ulimates complete. #records processed: ' + records.toString();
    } catch (err) {
      console.log('error:', err);
      return err;
    }
  }

  async fixMissingPlayers() {
    try {
      console.log('Fixing missing players...');
      var tempPlayersResp = await fetch(`http://xperimental.io:4200/api/v1/get/pff/player_list_ids/`);
      var tempPlayersJSON = await tempPlayersResp.json();
      var tempPlayers = tempPlayersJSON.qbs;
      var records = 0;

      var tempDetailsResp = await fetch(`http://xperimental.io:4200/api/v1/get/pff/player_details/`);
      var tempDetailsJSON = await tempDetailsResp.json();
      var tempDetails = tempDetailsJSON.qbs;

      for(var tp=0;tp<tempPlayers.length;tp++) {
        var player = tempPlayers[tp];
        var detailIndex = tempDetails.findIndex((v,i) => {
          return v.player_id === player.player_id;
        });

        if(detailIndex === -1) {
          // Oops! No player info available, let's try and re-grab it!
          var playerInfoResp = await fetch(`https://premium.pff.com/api/v1/players?league=nfl&id=${player.player_id}`);
          var playerInfoJSON = await playerInfoResp.json();
          var playerInfo = playerInfoJSON.players[0];

          var tplayer = {
            pff_id: playerInfo.id,
            first_name: playerInfo.first_name,
            last_name: playerInfo.last_name,
            full_name: player.player,
            birthday: playerInfo.dob,
            hometown: ''
          }; 

          var pff_player_insert_r = await fetch(`http://xperimental.io:4200/api/v1/add/pff/player/`, {
            method: 'post',              
            headers: {
              'Accept': 'application/json',
              'Content-Type': 'application/json'
            },
            body: JSON.stringify(tplayer)
          });
          var pff_player_insert = await pff_player_insert_r.json();
          if(pff_player_insert.success) {
            console.log(`Added missing player: ${tplayer.full_name}. ID: ${tplayer.pff_id}`);
          }
          records++;
        }
      }
      return 'players fixed probably. #records processed: ' + records.toString();
    } catch (err) {
      console.log('error:', err);
      return err;
    }
  }

  async minorFixes() {
    try {
      var tempPlayersResp = await fetch(`http://xperimental.io:4200/api/v1/get/pff/player_list_ids/`);
      var tempPlayersJSON = await tempPlayersResp.json();
      var tempPlayers = tempPlayersJSON.qbs;

      for (var t=0;t<tempPlayers.length;t++) {
        var player = tempPlayers[t];

        var playerInfoResp = await fetch(`https://premium.pff.com/api/v1/players?league=nfl&id=${player.player_id}`);
        var playerInfoJSON = await playerInfoResp.json();
        var playerInfo = playerInfoJSON.players[0];
        var tplayer = {
          pff_id: player.player_id,
          first_name: playerInfo.first_name,
          last_name: playerInfo.last_name,
          full_name: player.player,
          birthday: playerInfo.dob,
          hometown: ''
        };

        var pff_player_insert_r = await fetch(`http://xperimental.io:4200/api/v1/add/pff/player/`, {
          method: 'post',              
          headers: {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
          },
          body: JSON.stringify(tplayer)
        });
        var pff_player_insert = await pff_player_insert_r.json();
      }
    } catch (err) {
      //eslint-disable-next-line
      console.log('error:', err);
      return err;
    }  
  }

  async addDatesForGames() {
    try {
      var records = 0;
      // Need to get things in this order: teams, games, players, player_stats, historical calcs
      for(var s=2008;s<=2020;s++) {
        for(var w=1;w<=21;w++) {  
          if(s === 2020 && w > 3) continue;
          var gamesDataResp = await fetch(`https://premium.pff.com/api/v1/games?season=${s}&week=${w}&league=nfl`);
          var gamesDataJSON = await gamesDataResp.json();
          var gamesData = gamesDataJSON.games;

          for(var g=0;g<gamesData.length;g++) {
            var game = gamesData[g];
            var game = {
              season: s,
              week: w,
              stadium_id: game.stadium_id,
              pff_id: game.id,
              away_team_id: game.away_franchise_id,
              home_team_id: game.home_franchise_id,
              game_date: moment(game.start).format('YYYY-MM-DD')
            };

            var pff_game_insert_r = await fetch(`http://xperimental.io:4200/api/v1/update/pff/game/date`, {
              method: 'post',              
              headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json'
              },
              body: JSON.stringify(game)
            });
            var pff_game_insert = await pff_game_insert_r.json();
            if(pff_game_insert) {
              var gn = g+1;
              //console.log(`updated game # ${gn} for ${w} - ${s}...`);
            }
          }

          console.log(`completed games for ${w} - ${s}...`);
          records++;
        }
      }

      return 'game dates added. #records processed: ' + records.toString();
    } catch (err) {
      console.log('error:', err);
      return err;
    }
  }

  async megaFill() {
    // OK, found all the endpoints I need from PFF...    
    try {
      // Need to get things in this order: teams, games, players, player_stats, historical calcs
      for(var s=2019;s<=2020;s++) {
        console.log('starting season ' + s.toString() + '...');
        // Add team data, this works at the season level, so do it before week processing
        if(s === 2020) {
          var teamsDataResp = await fetch(`https://premium.pff.com/api/v1/teams?season=${s}&league=nfl`);
          var teamsDataJSON = await teamsDataResp.json();
          var teamsData = teamsDataJSON.teams;
          console.log('starting teams...');
          for(var t=0;t<teamsData.length;t++) {
            var pff_team = teamsData[t];
            var team = {
              abbreviation: pff_team.abbreviation,
              city: pff_team.city,
              nickname: pff_team.nickname,
              franchise_id: pff_team.franchise_id,
              primary_color: pff_team.primary_color,
              secondary_color: pff_team.secondary_color,
              season: s,
              color_metadata: pff_team.color_metadata,
              conference: pff_team.groups[0].name,
              division: pff_team.groups[1].name.split(' ')[1]
            };

            var pff_team_insert_r = await fetch(`http://xperimental.io:4200/api/v1/add/pff/team/`, {
                method: 'post',              
                headers: {
                  'Accept': 'application/json',
                  'Content-Type': 'application/json'
                },
                body: JSON.stringify(team)
              });
              var pff_team_insert = await pff_team_insert_r.json();
              if(pff_team_insert) {
                console.log('added ' + team.nickname + ' for ' + s.toString());
              }
          }    

          console.log('teams done for season...'); 
        }

        // This should be out here.
        var tempPlayers = {};
        // At the week level we will do games and stats
        for(var w=1;w<=21;w++) {   
          if(s === 2019 && w < 11) continue;
          console.log(`starting week ${w} - ${s}...`);     
          var gamesDataResp = await fetch(`https://premium.pff.com/api/v1/games?season=${s}&week=${w}&league=nfl`);
          var statsDataResp = await fetch(`https://www.pff.com/api/fantasy/stats/passing?&season=${s}&weeks=${w}`);
          var advDataResp = await fetch(`https://premium.pff.com/api/v1/facet/passing/summary?league=nfl&season=${s}&week=${w}`);
          
          var gamesDataJSON = await gamesDataResp.json();
          var statsData = await statsDataResp.json();
          var advDataJSON = await advDataResp.json();
          var advData = advDataJSON.passing_stats;
          var gamesData = gamesDataJSON.games;
    
          console.log(`starting games for ${w} - ${s}...`);
          for(var g=0;g<gamesData.length;g++) {
            var game = gamesData[g];
            var game = {
              score_away: game.score.away_team,
              score_home: game.score.home_team,
              season: s,
              week: w,
              stadium_id: game.stadium_id,
              pff_id: game.id,
              away_team_id: game.away_franchise_id,
              home_team_id: game.home_franchise_id,
              winner: game.score.away_team > game.score.home_team ? 'away' : 'home',
              winner_id: game.score.away_team > game.score.home_team ? game.away_franchise_id : game.home_franchise_id,
              loser_id: game.score.away_team > game.score.home_team ? game.home_franchise_id : game.away_franchise_id,
            };

            var pff_game_insert_r = await fetch(`http://xperimental.io:4200/api/v1/add/pff/game/`, {
              method: 'post',              
              headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json'
              },
              body: JSON.stringify(game)
            });
            var pff_game_insert = await pff_game_insert_r.json();
            if(pff_game_insert) {
              var gn = g+1;
              console.log(`added game # ${gn} for ${w} - ${s}...`);
            }
          }

          console.log(`completed games for ${w} - ${s}...`);
          
          console.log(`starting stats for ${w} - ${s}...`);
          for(var p=0;p<statsData.length;p++) {
            var stat_line = statsData[p];
            var player = false;

            // Setup some basic caching for players, since we don't want to fetch duplicates
            if(tempPlayers[stat_line.player_id]) {
              player = tempPlayers[stat_line.player_id];
            } else {
              var playerInfoResp = await fetch(`https://premium.pff.com/api/v1/players?league=nfl&id=${stat_line.player_id}`);
              var playerInfoJSON = await playerInfoResp.json();
              var playerInfo = playerInfoJSON.players[0];
              player = {
                pff_id: stat_line.player_id,
                first_name: playerInfo.first_name,
                last_name: playerInfo.last_name,
                full_name: stat_line.player,
                birthday: playerInfo.dob,
                hometown: ''
              };

              tempPlayers[stat_line.player_id] = player;
            }

            var advPlayerIndex = advData.findIndex((v, i) => {
              return v.player_id === stat_line.player_id;
            });

            if(advPlayerIndex === -1) {
              console.log('bad stat line for player: ', stat_line.player);
              continue;
            }

            var advPlayer = advData[advPlayerIndex];
            var week = {
              player: stat_line.player,
              player_id: stat_line.player_id,
              att: stat_line.att,
              comp: stat_line.comp,
              dropbacks: stat_line.dropbacks,
              fumbles: stat_line.fumbles,
              games: stat_line.games,
              ints: stat_line.ints,
              qbr: parseFloat(advPlayer.qb_rating),
              ypa: advPlayer.ypa,
              comp_per: parseFloat(advPlayer.completion_percent),
              rush_carries:stat_line.rush_carries,
              rush_tds: stat_line.rush_tds,
              rush_yds: stat_line.rush_yds,
              tds: stat_line.tds,
              team: stat_line.team,
              team_id: stat_line.team_id,
              yds: stat_line.yds,
              jake_score: (parseInt(stat_line.ints) + parseInt(stat_line.fumbles)) * (1/6),
              season: s,
              week: w,
              sacks: stat_line.sks,
              ultimate_score: 0.00
            };          

            week.ultimate_score = this.calculateUltimate(week, player.birthday);
            var pff_week_insert_r = await fetch(`http://xperimental.io:4200/api/v1/add/pff/week/`, {
              method: 'post',              
              headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json'
              },
              body: JSON.stringify(week)
            });
            var pff_week_insert = await pff_week_insert_r.json();      
          } // End Stat Loop

          console.log(`completed stats for ${w} - ${s}...`);
        } // End Week Loop

        console.log(`completed season ${s}...`);
      } // End Season Loop

      console.log(`completed majority of data addition!`);
      // Insert players after all other data is process
      console.log(`starting all players ${s}...`);
      for (var playerId in tempPlayers) {
        var player = tempPlayers[playerId];
        var pff_player_insert_r = await fetch(`http://xperimental.io:4200/api/v1/add/pff/player/`, {
          method: 'post',              
          headers: {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
          },
          body: JSON.stringify(player)
        });
        var pff_player_insert = await pff_player_insert_r.json();
      }

      console.log('Finished adding all data successfully!');
    } catch (err) {
      //eslint-disable-next-line
      console.log('error:', err);
      return err;
    }      
  }

  async fixNFLTeamHistory() {
    try { 
      var fixResp = await fetch(`http://xperimental.io:4200/api/v1/fix/nfl/teams/`);
      var fixJSON = await fixResp.json();
      console.log(fixJSON);
      if(fixJSON.success) {
        return 'Finished the team update. Maybe. Probably though. Probably.';
      } else {
        return 'Something shit the bed.';
      }
    } catch (err) {
      //eslint-disable-next-line
      console.log('error:', err);
      return err;
    }
  }

  async linkPFFToNFL() {
    try {
      for(var s=2019;s>=2009;s--) {
        var listResp = await fetch(`http://xperimental.io:4200/api/v1/get/pff/player_list/${s}`);
        var playerListJSON = await listResp.json();
        var season_qbs = playerListJSON.qbs;

        for(var sq=0;sq<season_qbs.length;sq++) {
          var pff_qb = season_qbs[sq];
          var nfl_player_url = new URL(`http://xperimental.io:4200/api/v1/get/nfl/player`)
          var params = {season: s, player: pff_qb.player, position: 'QB', status: 'ACT'};
          nfl_player_url.search = new URLSearchParams(params).toString();

          var nfl_qb_resp = await fetch(nfl_player_url);
          var nfl_qb_json = await nfl_qb_resp.json();
          var nfl_qb = nfl_qb_json.player;       

          if(nfl_qb && nfl_qb.nflId) {
            var insertQB = {
              pff_id: 0,
              nfl_id: nfl_qb.nflId,
              first_name: pff_qb.player.split(' ')[0],
              last_name: pff_qb.player.split(' ')[1],
              full_name: pff_qb.player,
              birthday: nfl_qb.birthDate,
              hometown: nfl_qb.homeTown
            };

            var nfl_qb_resp = await fetch(`http://xperimental.io:4200/api/v1/add/pff/player/`, {
              method: 'post',              
              headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json'
              },
              body: JSON.stringify(insertQB)
            });
            var nfl_qb_json = await nfl_qb_resp.json();
          }
        }
      }

      return 'Finished the player update. Maybe. Probably though. Probably.';
    } catch (err) {
      //eslint-disable-next-line
      console.log('error:', err);
      return err;
    }
  }

  async updateCurrentWeek() {
    try {
      var latestWeekResp = await fetch(`http://xperimental.io:4200/api/v1/pff/get/currentweek/${this.season}`);
      var latestWeek = await latestWeekResp.json();

      // Then we have data and need to update instead of insert.
      if(latestWeek.week && parseInt(latestWeek.week) > 0) {
        debugger;
        var l_games_resp = await fetch(`http://xperimental.io:4200/api/v1/get/pff/games/${this.season}/${latestWeek.week}`);
        var l_games = await l_games_resp.json();

        var l_players_resp = await fetch(`http://xperimental.io:4200/api/v1/get/pff/players/${this.season}/${latestWeek.week}`);
        var l_players = await l_players_resp.json();

        var pff_players_response = await fetch(`https://www.pff.com/api/fantasy/stats/passing?&season=${this.season}&weeks=${latestWeek.week}`);      
        var pff_players_weekdata = await pff_players_response.json();
        console.log('wd:', pff_players_weekdata);

        var pff_games_response = await fetch(`https://premium.pff.com/api/v1/games?week=${latestWeek.week}&season=${this.season}&league=nfl`);
        var pff_games = await pff_games_response.json();
        
        // OK, we need to update players and games, if they match - we don't care if they changed, just update anyway.
        // Player Update first.
        for(var pfp=0;pfp<pff_players_weekdata.length;pfp++) {
          var pff_player = pff_players_weekdata[pfp];

          for(var lp=0;lp<l_players.length;lp++) {
            var local_player = l_players[lp];
            if(local_player.player_id !== pff_player.player_id) continue;

            var updated_resp = await fetch(`http://xperimental.io:4200/api/v1/update/pff/week/`, {
              method: 'post',              
              headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json'
              },
              body: JSON.stringify(pff_player)
            });

            var updated = await updated_resp.json();
            if(!updated.success) console.log('failed to update: ', local_player)
          }
        }       
        
        // Then update games
        for(var pfg=0;pfg<pff_games.length;pfg++) {
          var pff_game = pff_games[pfg];

          for(var lg=0;lg<l_games.length;lg++) {
            var local_game = l_games[lg];
            if(local_game.away_team_id !== pff_game.away_team_id || local_game.home_team_id !== pff_game.home_team_id) continue;
            pff_game['season'] = local_game.season;
            pff_game['week'] = local_game.week;

            var updated_resp = await fetch(`http://xperimental.io:4200/api/v1/update/pff/game/score`, {
              method: 'post',
              headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json'
              },
              body: JSON.stringify(pff_game)
            });

            var updated = await updated_resp.json();
            if(!updated.success) console.log('failed to update: ', local_game)
          }
        }   
      }

      return 'probably updated...';
    } catch (err) {
      //eslint-disable-next-line
      console.log('error:', err);
      return err;
    }
  }

  async fetchPFF() {
    var season = 2008;
    var weeks = 21;

    console.log('starting pff fetch...');
    for(var s=season;s<=2020;s++) {
      console.log(`starting season ${s}...`);
      if(s < 2020) weeks = 21;
      if(s >= 2020) weeks = 2;

      for(var w=1;w<=weeks;w++) {
        console.log(`starting week ${w}...`);
        var response = await fetch(`https://www.pff.com/api/fantasy/stats/passing?&season=${s}&weeks=${w}`);
        var weekdata = await response.json();
        
        if(weekdata) {
          //console.log('weekdata:', weekdata);
          //return;

          for(var p=0;p<weekdata.length;p++) {
            var playerStats = weekdata[p];
            var ints = parseInt(playerStats['ints']);
            var fumbles = parseInt(playerStats['fumbles']);
            var jakeMulti = 1/6;

            playerStats['week'] = w;
            playerStats['season'] = s;
            playerStats['jake_score'] = parseFloat((ints + fumbles) * jakeMulti);
            
            let playerInsert = await fetch('http://xperimental.io:4200/api/v1/add/pff/week', {
              method: 'post',
              headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json'
              },
              body: JSON.stringify(playerStats)
            });
      
            let playerOK = await playerInsert.json();
            if(!playerOK){
              console.log('failed to add:', playerStats);
            }
          }
        }
      }        
    }

    return 'Added all data OK, probably.';
  }

  async fetchPFFGameData() {
    var season = 2008;
    var weeks = 21;

    console.log('starting pff game fetch...');
    for(var s=season;s<=2020;s++) {
      console.log(`starting season ${s}...`);
      if(s < 2020) weeks = 21;
      if(s >= 2020) weeks = 2;

      for(var w=1;w<=weeks;w++) {
        console.log(`starting week ${w}...`);
        var response = await fetch(`https://premium.pff.com/api/v1/games?week=${w}&season=${s}&league=nfl`);
        var weekjson = await response.json();
        var weekdata = weekjson.games;

        if(weekdata) {
          for(var p=0;p<weekdata.length;p++) {
            let gameData = weekdata[p];
            let awayScore = parseInt(gameData.score.away_team);
            let homeScore = parseInt(gameData.score.home_team);
            let winner = '';
            let loser_id = 0;
            let winner_id = 0;

            if(awayScore === homeScore) {
              winner = 'tie';
            } else {
              if (awayScore > homeScore) {
                winner = 'away';
                loser_id = gameData.home_team.franchise_id;
                winner_id = gameData.away_team.franchise_id;
              } else {
                winner = 'home';
                loser_id = gameData.away_team.franchise_id;
                winner_id = gameData.home_team.franchise_id;
              }
            }

            let gameInsertData = {
              score_away: gameData.score.away_team,
              score_home: gameData.score.home_team,
              season: s, 
              week: w, 
              stadium_id: gameData.stadium_id,
              away_team_id: gameData.away_team.franchise_id,
              home_team_id: gameData.home_team.franchise_id,
              winner: winner,
              winner_id: winner_id,
              loser_id: loser_id
            };

            let gameInsert = await fetch('http://xperimental.io:4200/api/v1/add/pff/game', {
              method: 'post',
              headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json'
              },
              body: JSON.stringify(gameInsertData)
            });
      
            let gameOK = await gameInsert.json();
            if(!gameOK){
              console.log('failed to add:', gameInsertData);
            }
          }
        }
      }        
    }

    return 'Added all game data OK, probably.';
  }

  async fillNFLPlayersWithPFF() {
    var s = 2020;
    var qbresponse = await fetch(`http://xperimental.io:4200/api/v1/get/pff/player_list/${s}`);
    var qbjson = await qbresponse.json();
    var qbs = qbjson.qbs;

    console.log('qbs data:', qbs);
    
    for(var q=0;q<qbs.length;q++) {
      var qb = qbs[q];
      var playerName = qb.player;

      console.log('qb data:', qb);

      var plresponse = await fetch(`http://xperimental.io:4200/api/v1/get/player/name/`,{
        method: 'post',              
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ name: playerName, season: 2019 })
      });

      var pljson = await plresponse.json();
      var playerInfo = pljson.player ? pljson.player : null;

      console.log('playerInfo data:', playerInfo);

      var insertData = {
        season: s,
        nflId: playerInfo ? playerInfo.nflId : 0,
        displayName: playerName,
        firstName: playerName.split(' ')[0],
        lastName: playerName.split(' ')[1],
        positionGroup: 'QB',
        position: 'QB',
        birthDate: playerInfo ? playerInfo.birthDate : '',
        jerseyNumber: playerInfo ? playerInfo.jerseyNumber : '',
        height: playerInfo ? playerInfo.height : '',
        weight: playerInfo ? playerInfo.weight : '',
      };  
   
      var added_playerr = await fetch(`http://xperimental.io:4200/api/v1/add/player`, {
        method: 'post',              
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(insertData)
      });
      var added_playerj = await added_playerr.json();
      var update_pff_data = {
        player_id: added_playerj.id,
        id: qb.id
      };

      console.log('pff data:', update_pff_data);

      var update_pff_idr = await fetch(`http://xperimental.io:4200/api/v1/update/pff/player/`, {
        method: 'post',              
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(update_pff_data)
      });

      var update_pff_idj = await update_pff_idr.json();
      console.log('pff resp:', update_pff_idj);

      if(!update_pff_idj.success) {
        console.log('failed, last data:', update_pff_data);
      }
    }
  }

  async fetchPFFTeamData() {
    var season = 2008;

    console.log('starting pff team fetch...');
    for(var s=season;s<=2020;s++) {
      console.log(`starting season ${s}...`);
    
      var response = await fetch(`https://www.pff.com/api/fantasy/stats/teams?league=nfl&season=${s}`);
      var seasonjson = await response.json();
      var seasondata = seasonjson.teams;
      
      if(seasondata) {
        for(var sd=0;sd<seasondata.length;sd++) {
          let teamData = seasondata[sd];
          let teamInsertData = {
            abbreviation: teamData.abbreviation,
            city: teamData.city,
            franchise_id: teamData.franchise_id,
            nickname: teamData.nickname,
            primary_color: teamData.primary_color,
            secondary_color: teamData.secondary_color,
            season: s
          };

          let teamInsert = await fetch('http://xperimental.io:4200/api/v1/add/pff/team', {
            method: 'post',
            headers: {
              'Accept': 'application/json',
              'Content-Type': 'application/json'
            },
            body: JSON.stringify(teamInsertData)
          });
    
          let teamOK = await teamInsert.json();
          if(!teamOK){
            console.log('failed to add:', teamInsertData);
          }
        }
      }    
    }

    return 'Added all team data OK, probably.';
  }
  

  async fixGamesOld() {
    // Due to an oversight...I have to add in a bunch of game stats.    
    let gamesReq = await fetch('http://xperimental.io:4200/api/v1/get/games/all');
    let gjson = await gamesReq.json();
    let games = gjson.games;

    for (let g=0;g<games.length;g++) {
      let game = games[g];
      let gameReq = await fetch(this.feedBaseURL + 'boxscore/' + game.gameId.toString() + '.json');
      var gameBox = await gameReq.json();

      var gameObj = {        
        homeWin: parseInt(gameBox.score.homeTeamScore.pointTotal) > parseInt(gameBox.score.visitorTeamScore.pointTotal),
        visitorWin: parseInt(gameBox.score.visitorTeamScore.pointTotal) > parseInt(gameBox.score.homeTeamScore.pointTotal)
      };

      //eslint-disable-next-line
      console.log('Fixing game for season: ' + game.season + ' week: ' + game.week);

      var scoreObj = {
        gameId: game.gameId,
        phase: gameBox.score.phase,
        homePointTotal: gameBox.score.homeTeamScore.pointTotal,
        homePointQ1: gameBox.score.homeTeamScore.pointQ1,
        homePointQ2: gameBox.score.homeTeamScore.pointQ2,
        homePointQ3: gameBox.score.homeTeamScore.pointQ3,
        homePointQ4: gameBox.score.homeTeamScore.pointQ4,
        visitorPointTotal: gameBox.score.visitorTeamScore.pointTotal,
        visitorPointQ1: gameBox.score.visitorTeamScore.pointQ1,
        visitorPointQ2: gameBox.score.visitorTeamScore.pointQ2,
        visitorPointQ3: gameBox.score.visitorTeamScore.pointQ3,
        visitorPointQ4: gameBox.score.visitorTeamScore.pointQ4
      };

      let dataPack = {
        game: gameObj,
        score: scoreObj,
        id: game.id
      };

      let gameUpdReq = await fetch('http://xperimental.io:4200/api/v1/add/game/score', {
        method: 'post',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(dataPack)
      });

      let gameUpdResp = await gameUpdReq.json();
    }
  }

  async fillDB() {
    // Lets start by getting all the team data
    var teamResp = await fetch(this.feedBaseURL + 'teams/' + this.currentYear.toString() + '.json');
    var teamJSON = await teamResp.json();

    //console.log('team json:', teamJSON);
    //return;
    //var teamsDone = await this.teamSetup(teamJSON.teams);
    let teamsDone = true;

    if (teamsDone) {
      // eslint-disable-next-line
      //console.log('Team building complete!');

      // NOTE: Something broke after 2013 so latter stats are not complete yet!!


      for (let y = 2020; y <= 2020; y++) {
        //        
        let rosterRes = true;       
        //rosterRes = await this.rosterSetup(y, teamJSON.teams);
        
        //let rosterRes = true;
        if (!rosterRes) {
          // eslint-disable-next-line
          console.log('Failed to retrieve roster(s).');
          return;
        }

        // eslint-disable-next-line
        //console.log('Rosters complete for ' + y.toString());

        // Then we'll get all the game data
        let res = await this.getSchedule(y);
        if (!res) {
          // eslint-disable-next-line
          console.log('Failed to retrieve schedule data for year: ' + y.toString());
          return;
        }
        
        // eslint-disable-next-line
        console.log('Retrieved schedule for ' + y.toString());
      }      

      // Should have all the schedules for all years now stored.
      for (var year in this.schedules) {
        if (typeof this.preSeasons[year] === 'undefined') this.preSeasons[year] = [];
        if (typeof this.regSeasons[year] === 'undefined') this.regSeasons[year] = [];
        if (typeof this.postSeasons[year] === 'undefined') this.postSeasons[year] = [];
        let yearData = this.schedules[year];

        /*
        for (var preweek in yearData.pre) {
          if (typeof this.preSeasons[year][preweek] === 'undefined') this.preSeasons[year][preweek] = [];

          let weekGames = yearData.pre[preweek];
          for (let g = 0; g < weekGames.length; g++) {
            let gameid = weekGames[g];
            let gameResp = await fetch(this.feedBaseURL + 'boxscore/' + gameid.toString() + '.json');
            let gameData = await gameResp.json();
            let gameAdded = await this.gameSetup(gameData, 'pre');

            if (gameAdded) {
              // eslint-disable-next-line
              console.log('Added game: ' + gameData.gameSchedule.homeTeamAbbr +
                ' vs. ' + gameData.gameSchedule.visitorTeamAbbr + '. Week ' +
                gameData.gameSchedule.week + ' in ' + gameData.gameSchedule.season);
            }
          }
        }
        */

        
        for (var regweek in yearData.reg) {
          if (typeof this.regSeasons[year][regweek] === 'undefined') this.regSeasons[year][regweek] = [];

          let weekGames = yearData.reg[regweek];
          for (let g = 0; g < weekGames.length; g++) {
            let gameid = weekGames[g];
            let gameResp = await fetch(this.feedBaseURL + 'boxscore/' + gameid.toString() + '.json');
            let gameData = await gameResp.json();
            let gameAdded = await this.gameSetup(gameData, 'reg');

            if (gameAdded) {
              // eslint-disable-next-line
              /*
              console.log('Added game: ' + gameData.gameSchedule.homeTeamAbbr +
                ' vs. ' + gameData.gameSchedule.visitorTeamAbbr + '. Week ' +
                gameData.gameSchedule.week + ' in ' + gameData.gameSchedule.season); */
            }
          }
        }

        // eslint-disable-next-line
        console.log('Regular Season: ' + year.toString() + ' is complete.');

        for (var postweek in yearData.post) {
          if (typeof this.postSeasons[year][postweek] === 'undefined') this.postSeasons[year][postweek] = [];

          let weekGames = yearData.post[postweek];
          for (let g = 0; g < weekGames.length; g++) {
            let gameid = weekGames[g];
            let gameResp = await fetch(this.feedBaseURL + 'boxscore/' + gameid.toString() + '.json');
            let gameData = await gameResp.json();
            let gameAdded = await this.gameSetup(gameData, 'post');

            if (gameAdded) {
              // eslint-disable-next-line
              /*
              console.log('Added game: ' + gameData.gameSchedule.homeTeamAbbr +
                ' vs. ' + gameData.gameSchedule.visitorTeamAbbr + '. Week ' +
                gameData.gameSchedule.week + ' in ' + gameData.gameSchedule.season);*/
            }
          }
        }
        
        // eslint-disable-next-line
        console.log('Post Season: ' + year.toString() + ' is complete.');
      }

      return 'Added all data OK, probably.';
    } else {
      return 'Teams failed.'
    }
  }

  async gameSetup(gameData, seasonType) {
    if (typeof gameData !== 'undefined') {
      let gameInitData = {
        season: gameData.gameSchedule.season,
        seasonType: seasonType,
        week: gameData.gameSchedule.week,
        gameId: gameData.gameSchedule.gameId,
        gameKey: gameData.gameSchedule.gameKey,
        gameDate: gameData.gameSchedule.gameDate,
        gameTimeEastern: gameData.gameSchedule.gameTimeEastern,
        gameTimeLocal: gameData.gameSchedule.gameTimeLocal,
        isoTime: gameData.gameSchedule.isoTime,
        homeTeamId: gameData.gameSchedule.homeTeamId,
        visitorTeamId: gameData.gameSchedule.visitorTeamId,
      };

      

      let gameReq = await fetch('http://xperimental.io:4200/api/v1/add/game/main', {
        method: 'post',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(gameInitData)
      });

      let gameResp = await gameReq.json();
      if (true) {

        let aggregatesDone = await this.gameAggregateSetup(gameData, seasonType);
        //let defensiveDone = await this.gameDefensiveSetup(gameData, seasonType);
        let fumblesDone = await this.gameFumblesSetup(gameData, seasonType);
        //let kickingDone = await this.gameKickingSetup(gameData, seasonType);
        let passingDone = await this.gamePassingSetup(gameData, seasonType);
        //let puntingDone = await this.gamePuntingSetup(gameData, seasonType);
        //let receivingDone = await this.gameReceivingSetup(gameData, seasonType);
        //let returnDone = await this.gameReturnSetup(gameData, seasonType);
        //let rushingDone = await this.gameRushingSetup(gameData, seasonType);
        //let done = (defensiveDone && kickingDone && puntingDone && receivingDone && returnDone && rushingDone);
        let done = aggregatesDone && fumblesDone && passingDone;
        return done;
      }
    }

    return false;
  }

  async gameAggregateSetup(gameData, seasonType) {
    let aggregateInit = {
      season: gameData.gameSchedule.season,
      seasonType: seasonType,
      week: gameData.gameSchedule.week,
      gameId: gameData.gameSchedule.gameId,
      teamId: ''
    };

    // Insert Home Team Aggregate Info
    let aggregateHomeData = {
      ...aggregateInit,
      ...gameData.homeTeamStats
    };
    aggregateHomeData.teamId = gameData.gameSchedule.homeTeamId;

    let aggReqHome = await fetch('http://xperimental.io:4200/api/v1/add/game/aggregate', {
      method: 'post',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        },
      body: JSON.stringify(aggregateHomeData)
    });

    let aggRespHome = await aggReqHome.json();

    // Insert Visitor Team Aggregate Info
    let aggregateVisitorData = {
      ...aggregateInit,
      ...gameData.visitorTeamStats
    };
    aggregateVisitorData.teamId = gameData.gameSchedule.visitorTeamId;

    let aggReqVisitor = await fetch('http://xperimental.io:4200/api/v1/add/game/aggregate', {
      method: 'post',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        },
      body: JSON.stringify(aggregateVisitorData)
    });

    let aggRespVisitor = await aggReqVisitor.json();

    return aggRespHome.success && aggRespVisitor.success;
  }

  async gameDefensiveSetup(gameData, seasonType) {
    // Insert Defensive Stats
    let defensiveInit = {
      season: gameData.gameSchedule.season,
      seasonType: seasonType,
      week: gameData.gameSchedule.week,
      gameId: gameData.gameSchedule.gameId,
      teamId: 0,
      isHome: false,
      isVisitor: false,
      playerId: 0
    };

    // Home Defense First
    let homeDefense = gameData.homeTeamBoxScoreStat.playerBoxScoreDefensiveStats;

    // Gotta add alllllllllllllllllllllllllllll the players
    for (let hd = 0; hd < homeDefense.length; hd++) {
      let hdPlayer = homeDefense[hd].teamPlayer;
      let hdPlayerStats = homeDefense[hd].playerGameStat.playerDefensiveStat;
      let homeDefensiveData = {
        ...defensiveInit,
        ...hdPlayerStats
      };

      homeDefensiveData.isHome = true;
      homeDefensiveData.teamId = gameData.gameSchedule.homeTeamId;
      homeDefensiveData.playerId = hdPlayer.nflId;
      await fetch('http://xperimental.io:4200/api/v1/add/game/defensive', {
        method: 'post',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(homeDefensiveData)
      });

      //let homeDefResp = await homeDefReq.json();
    }

    // Visitor Defense First
    let visitorDefense = gameData.visitorTeamBoxScoreStat.playerBoxScoreDefensiveStats;

    // Gotta add alllllllllllllllllllllllllllll the players
    for (let vd = 0; vd < visitorDefense.length; vd++) {
      let vdPlayer = visitorDefense[vd].teamPlayer;
      let vdPlayerStats = visitorDefense[vd].playerGameStat.playerDefensiveStat;
      let visitorDefensiveData = {
        ...defensiveInit,
        ...vdPlayerStats
      };

      visitorDefensiveData.isVisitor = true;
      visitorDefensiveData.teamId = gameData.gameSchedule.visitorTeamId;
      visitorDefensiveData.playerId = vdPlayer.nflId;
      await fetch('http://xperimental.io:4200/api/v1/add/game/defensive', {
        method: 'post',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(visitorDefensiveData)
      });

      //let visitorDefResp = await visitorDefReq.json();
    }
  }

  async gameFumblesSetup(gameData, seasonType) {
    // Insert Defensive Stats
    let fumblesInit = {
      season: gameData.gameSchedule.season,
      seasonType: seasonType,
      week: gameData.gameSchedule.week,
      gameId: gameData.gameSchedule.gameId,
      teamId: 0,
      isHome: false,
      isVisitor: false,
      playerId: 0
    };

    // Home Fumbles First
    let homeFumbles = gameData.homeTeamBoxScoreStat.playerBoxScoreFumbleStats;

    // Gotta add alllllllllllllllllllllllllllll the players
    for (let hd = 0; hd < homeFumbles.length; hd++) {
      let hdPlayer = homeFumbles[hd].teamPlayer;
      let hdPlayerStats = homeFumbles[hd].playerGameStat.playerFumbleStat;
      let homeFumblesData = {
        ...fumblesInit,
        ...hdPlayerStats
      };

      homeFumblesData.isHome = true;
      homeFumblesData.teamId = gameData.gameSchedule.homeTeamId;
      homeFumblesData.playerId = hdPlayer.nflId;
      await fetch('http://xperimental.io:4200/api/v1/add/game/fumbles', {
        method: 'post',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(homeFumblesData)
      });

      //let homeFumResp = await homeFumReq.json();
    }

    // Visitor Defense First
    let visitorFumbles = gameData.visitorTeamBoxScoreStat.playerBoxScoreFumbleStats;

    // Gotta add alllllllllllllllllllllllllllll the players
    for (let vd = 0; vd < visitorFumbles.length; vd++) {
      let vdPlayer = visitorFumbles[vd].teamPlayer;
      let vdPlayerStats = visitorFumbles[vd].playerGameStat.playerFumbleStat;
      let visitorFumblesData = {
        ...fumblesInit,
        ...vdPlayerStats
      };

      visitorFumblesData.isVisitor = true;
      visitorFumblesData.teamId = gameData.gameSchedule.visitorTeamId;
      visitorFumblesData.playerId = vdPlayer.nflId;
      await fetch('http://xperimental.io:4200/api/v1/add/game/fumbles', {
        method: 'post',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(visitorFumblesData)
      });

      //let visitorFumResp = await visitorFumReq.json();
    }

    return true;
  }

  async gameKickingSetup(gameData, seasonType) {
    // Insert Defensive Stats
    let kickingInit = {
      season: gameData.gameSchedule.season,
      seasonType: seasonType,
      week: gameData.gameSchedule.week,
      gameId: gameData.gameSchedule.gameId,
      teamId: 0,
      isHome: false,
      isVisitor: false,
      playerId: 0
    };

    // Home Defense First
    let homeKicking = gameData.homeTeamBoxScoreStat.playerBoxScoreKickingStats;

    // Gotta add alllllllllllllllllllllllllllll the players
    for (let hd = 0; hd < homeKicking.length; hd++) {
      let hdPlayer = homeKicking[hd].teamPlayer;
      let hdPlayerStats = homeKicking[hd].playerGameStat.playerKickingStat;
      let homeKickingData = {
        ...kickingInit,
        ...hdPlayerStats
      };

      homeKickingData.isHome = true;
      homeKickingData.teamId = gameData.gameSchedule.homeTeamId;
      homeKickingData.playerId = hdPlayer.nflId;
      await fetch('http://xperimental.io:4200/api/v1/add/game/kicking', {
        method: 'post',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(homeKickingData)
      });

      //let homeDefResp = await homeDefReq.json();
    }

    // Visitor Defense First
    let visitorKicking = gameData.visitorTeamBoxScoreStat.playerBoxScoreKickingStats;

    // Gotta add alllllllllllllllllllllllllllll the players
    for (let vd = 0; vd < visitorKicking.length; vd++) {
      let vdPlayer = visitorKicking[vd].teamPlayer;
      let vdPlayerStats = visitorKicking[vd].playerGameStat.playerKickingStat;
      let visitorKickingData = {
        ...kickingInit,
        ...vdPlayerStats
      };

      visitorKickingData.isVisitor = true;
      visitorKickingData.teamId = gameData.gameSchedule.visitorTeamId;
      visitorKickingData.playerId = vdPlayer.nflId;
      await fetch('http://xperimental.io:4200/api/v1/add/game/kicking', {
        method: 'post',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(visitorKickingData)
      });

      //let visitorDefResp = await visitorDefReq.json();
    }
  }

  async gamePassingSetup(gameData, seasonType) {
    // Insert Defensive Stats
    let passingInit = {
      season: gameData.gameSchedule.season,
      seasonType: seasonType,
      week: gameData.gameSchedule.week,
      gameId: gameData.gameSchedule.gameId,
      teamId: 0,
      isHome: false,
      isVisitor: false,
      playerId: 0
    };

    // Home Defense First
    let homePassing = gameData.homeTeamBoxScoreStat.playerBoxScorePassingStats;

    // Gotta add alllllllllllllllllllllllllllll the players
    for (let hd = 0; hd < homePassing.length; hd++) {
      let hdPlayer = homePassing[hd].teamPlayer;
      let hdPlayerStats = homePassing[hd].playerGameStat.playerPassingStat;
      let homePassingData = {
        ...passingInit,
        ...hdPlayerStats
      };

      homePassingData.isHome = true;
      homePassingData.teamId = gameData.gameSchedule.homeTeamId;
      homePassingData.playerId = hdPlayer.nflId;
      await fetch('http://xperimental.io:4200/api/v1/add/game/passing', {
        method: 'post',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(homePassingData)
      });

      //let homeDefResp = await homeDefReq.json();
    }

    // Visitor Defense First
    let visitorPassing = gameData.visitorTeamBoxScoreStat.playerBoxScorePassingStats;

    // Gotta add alllllllllllllllllllllllllllll the players
    for (let vd = 0; vd < visitorPassing.length; vd++) {
      let vdPlayer = visitorPassing[vd].teamPlayer;
      let vdPlayerStats = visitorPassing[vd].playerGameStat.playerPassingStat;
      let visitorPassingData = {
        ...passingInit,
        ...vdPlayerStats
      };

      visitorPassingData.isVisitor = true;
      visitorPassingData.teamId = gameData.gameSchedule.visitorTeamId;
      visitorPassingData.playerId = vdPlayer.nflId;
      await fetch('http://xperimental.io:4200/api/v1/add/game/passing', {
        method: 'post',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(visitorPassingData)
      });

      //let visitorDefResp = await visitorDefReq.json();
    }

    return true;
  }

  async gamePuntingSetup(gameData, seasonType) {
    // Insert Defensive Stats
    let puntingInit = {
      season: gameData.gameSchedule.season,
      seasonType: seasonType,
      week: gameData.gameSchedule.week,
      gameId: gameData.gameSchedule.gameId,
      teamId: 0,
      isHome: false,
      isVisitor: false,
      playerId: 0
    };

    // Home Defense First
    let homePunting = gameData.homeTeamBoxScoreStat.playerBoxScorePuntingStats;

    // Gotta add alllllllllllllllllllllllllllll the players
    for (let hd = 0; hd < homePunting.length; hd++) {
      let hdPlayer = homePunting[hd].teamPlayer;
      let hdPlayerStats = homePunting[hd].playerGameStat.playerPuntingStat;
      let homePuntingData = {
        ...puntingInit,
        ...hdPlayerStats
      };

      homePuntingData.isHome = true;
      homePuntingData.teamId = gameData.gameSchedule.homeTeamId;
      homePuntingData.playerId = hdPlayer.nflId;
      await fetch('http://xperimental.io:4200/api/v1/add/game/punting', {
        method: 'post',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(homePuntingData)
      });

      //let homeDefResp = await homeDefReq.json();
    }

    // Visitor Defense First
    let visitorPunting = gameData.visitorTeamBoxScoreStat.playerBoxScorePuntingStats;

    // Gotta add alllllllllllllllllllllllllllll the players
    for (let vd = 0; vd < visitorPunting.length; vd++) {
      let vdPlayer = visitorPunting[vd].teamPlayer;
      let vdPlayerStats = visitorPunting[vd].playerGameStat.playerPuntingStat;
      let visitorPuntingData = {
        ...puntingInit,
        ...vdPlayerStats
      };

      visitorPuntingData.isVisitor = true;
      visitorPuntingData.teamId = gameData.gameSchedule.visitorTeamId;
      visitorPuntingData.playerId = vdPlayer.nflId;
      await fetch('http://xperimental.io:4200/api/v1/add/game/punting', {
        method: 'post',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(visitorPuntingData)
      });

      //let visitorDefResp = await visitorDefReq.json();
    }

    return true;
  }

  async gameReceivingSetup(gameData, seasonType) {
    // Insert Defensive Stats
    let receivingInit = {
      season: gameData.gameSchedule.season,
      seasonType: seasonType,
      week: gameData.gameSchedule.week,
      gameId: gameData.gameSchedule.gameId,
      teamId: 0,
      isHome: false,
      isVisitor: false,
      playerId: 0
    };

    // Home Defense First
    let homeReceiving = gameData.homeTeamBoxScoreStat.playerBoxScoreReceivingStats;

    // Gotta add alllllllllllllllllllllllllllll the players
    for (let hd = 0; hd < homeReceiving.length; hd++) {
      let hdPlayer = homeReceiving[hd].teamPlayer;
      let hdPlayerStats = homeReceiving[hd].playerGameStat.playerReceivingStat;
      let homeReceivingData = {
        ...receivingInit,
        ...hdPlayerStats
      };

      homeReceivingData.isHome = true;
      homeReceivingData.teamId = gameData.gameSchedule.homeTeamId;
      homeReceivingData.playerId = hdPlayer.nflId;
      await fetch('http://xperimental.io:4200/api/v1/add/game/receiving', {
        method: 'post',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(homeReceivingData)
      });

      //let homeDefResp = await homeDefReq.json();
    }

    // Visitor Defense First
    let visitorReceiving = gameData.visitorTeamBoxScoreStat.playerBoxScoreReceivingStats;

    // Gotta add alllllllllllllllllllllllllllll the players
    for (let vd = 0; vd < visitorReceiving.length; vd++) {
      let vdPlayer = visitorReceiving[vd].teamPlayer;
      let vdPlayerStats = visitorReceiving[vd].playerGameStat.playerReceivingStat;
      let visitorReceivingData = {
        ...receivingInit,
        ...vdPlayerStats
      };

      visitorReceivingData.isVisitor = true;
      visitorReceivingData.teamId = gameData.gameSchedule.visitorTeamId;
      visitorReceivingData.playerId = vdPlayer.nflId;
      await fetch('http://xperimental.io:4200/api/v1/add/game/receiving', {
        method: 'post',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(visitorReceivingData)
      });

      //let visitorDefResp = await visitorDefReq.json();
    }
  }

  async gameReturnSetup(gameData, seasonType) {
    // Insert Defensive Stats
    let returnInit = {
      season: gameData.gameSchedule.season,
      seasonType: seasonType,
      week: gameData.gameSchedule.week,
      gameId: gameData.gameSchedule.gameId,
      teamId: 0,
      isHome: false,
      isVisitor: false,
      playerId: 0
    };

    // Home Defense First
    let homeReturn = gameData.homeTeamBoxScoreStat.playerBoxScoreReturnStats;

    // Gotta add alllllllllllllllllllllllllllll the players
    for (let hd = 0; hd < homeReturn.length; hd++) {
      let hdPlayer = homeReturn[hd].teamPlayer;
      let hdPlayerStats = homeReturn[hd].playerGameStat.playerReturnStat;
      let homeReturnData = {
        ...returnInit,
        ...hdPlayerStats
      };

      homeReturnData.isHome = true;
      homeReturnData.teamId = gameData.gameSchedule.homeTeamId;
      homeReturnData.playerId = hdPlayer.nflId;
      await fetch('http://xperimental.io:4200/api/v1/add/game/return', {
        method: 'post',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(homeReturnData)
      });

      //let homeDefResp = await homeDefReq.json();
    }

    // Visitor Defense First
    let visitorReturn = gameData.visitorTeamBoxScoreStat.playerBoxScoreReturnStats;

    // Gotta add alllllllllllllllllllllllllllll the players
    for (let vd = 0; vd < visitorReturn.length; vd++) {
      let vdPlayer = visitorReturn[vd].teamPlayer;
      let vdPlayerStats = visitorReturn[vd].playerGameStat.playerReturnStat;
      let visitorReturnData = {
        ...returnInit,
        ...vdPlayerStats
      };

      visitorReturnData.isVisitor = true;
      visitorReturnData.teamId = gameData.gameSchedule.visitorTeamId;
      visitorReturnData.playerId = vdPlayer.nflId;
      await fetch('http://xperimental.io:4200/api/v1/add/game/return', {
        method: 'post',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(visitorReturnData)
      });

      //let visitorDefResp = await visitorDefReq.json();
    }

    return true;
  }

  async gameRushingSetup(gameData, seasonType) {
    // Insert Defensive Stats
    let rushingInit = {
      season: gameData.gameSchedule.season,
      seasonType: seasonType,
      week: gameData.gameSchedule.week,
      gameId: gameData.gameSchedule.gameId,
      teamId: 0,
      isHome: false,
      isVisitor: false,
      playerId: 0
    };

    // Home Defense First
    let homeRushing = gameData.homeTeamBoxScoreStat.playerBoxScoreRushingStats;

    // Gotta add alllllllllllllllllllllllllllll the players
    for (let hd = 0; hd < homeRushing.length; hd++) {
      let hdPlayer = homeRushing[hd].teamPlayer;
      let hdPlayerStats = homeRushing[hd].playerGameStat.playerRushingStat;
      let homeRushingData = {
        ...rushingInit,
        ...hdPlayerStats
      };

      homeRushingData.isHome = true;
      homeRushingData.teamId = gameData.gameSchedule.homeTeamId;
      homeRushingData.playerId = hdPlayer.nflId;
      await fetch('http://xperimental.io:4200/api/v1/add/game/rushing', {
        method: 'post',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(homeRushingData)
      });

      //let homeDefResp = await homeDefReq.json();
    }

    // Visitor Defense First
    let visitorRushing = gameData.visitorTeamBoxScoreStat.playerBoxScoreRushingStats;

    // Gotta add alllllllllllllllllllllllllllll the players
    for (let vd = 0; vd < visitorRushing.length; vd++) {
      let vdPlayer = visitorRushing[vd].teamPlayer;
      let vdPlayerStats = visitorRushing[vd].playerGameStat.playerRushingStat;
      let visitorRushingData = {
        ...rushingInit,
        ...vdPlayerStats
      };

      visitorRushingData.isVisitor = true;
      visitorRushingData.teamId = gameData.gameSchedule.visitorTeamId;
      visitorRushingData.playerId = vdPlayer.nflId;
      await fetch('http://xperimental.io:4200/api/v1/add/game/rushing', {
        method: 'post',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(visitorRushingData)
      });

      //let visitorDefResp = await visitorDefReq.json();
    }
  }

  async getSchedule(y = 2001) {
    if (y > 2000) {
      const response = await fetch(this.feedBaseURL + 'schedules/' + y.toString() + '.json');
      const schedule = await response.json();

      if (typeof(schedule.gameSchedules) !== 'undefined') {
        if (schedule.gameSchedules.length > 0) {
          if (typeof(this.schedules[y]) === 'undefined') this.schedules[y] = [];
          var games = {
            pre: {},
            reg: {},
            post: {}
          };
          // Parse all the weeks in the schedule.
          schedule.gameSchedules.forEach((game) => {
            let st = game.seasonType.toLowerCase();
            if (st !== 'pro') {
            //console.log(st + ": " + game.week.toString());
              if (typeof games[st][game.week] === 'undefined') games[st][game.week] = [];
              // Skip the HoF game, b/c who the hell cares.
              if (game.seasonType === 'PRE' && parseInt(game.week) === 0) return;

              games[st][game.week].push(game.gameId);
            }
          });

          this.schedules[y] = games;
        }
      }
      //console.log('schedule:', schedule);
      //console.log('schedules:', this.schedules);
    }
    
    return Object.keys(this.schedules).length > 0;
  }

  async rosterSetup(rosterYear, teamData) {
    let success = 0;
    let playerCount = 0;

    for (var t = 0; t < teamData.length; t++) {
      let team = teamData[t];
      let req = await fetch(this.feedBaseURL + 'roster/' + team.teamId + '/' + rosterYear.toString() + '.json');
      let resp = await req.json();

      let roster = resp.teamPlayers;
      for (let r = 0; r < roster.length; r++) {
        let player = roster[r];
        let playerData = {};
        let badKeys = ['teamAbbr', 'teamSeq', 'teamFullName'];
        playerCount++;

        for (let playerKey in player) {
          if (badKeys.indexOf(playerKey) >= 0) {
            continue;
          }

          playerData[playerKey] = player[playerKey];
        }

        let insertReq = await fetch('http://xperimental.io:4200/api/v1/add/player', {
          method: 'post',
          headers: {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
          },
          body: JSON.stringify(playerData)
        });

        let ret = await insertReq.json();
        if (ret.success) {
          success++;
          //eslint-disable-next-line
          //console.log('Added player: ' + player.displayName + ' - ' + player.position + ' to player DB.');
        }
      }

      // eslint-disable-next-line
      console.log(team.abbr + ' roster for ' + rosterYear.toString() + ' season, is complete.');
    }

    return success === playerCount;
  }

  async teamSetup(teamData) {
    // So because teams move, and some teams came into existence and some left us, only to come back again later.
    // Though in our case we don't have to worry about it (???)
    let success = 0;
    for (var t = 0; t < teamData.length; t++) {
      let team = teamData[t];
      let teamModified = {
        season: this.currentYear,
        teamId: team.teamId,
        abbr: team.abbr,
        cityState: team.cityState,
        fullName: team.fullName,
        nick: team.nick,
        conferenceAbbr: team.conferenceAbbr,
        divisionAbbr: team.divisionAbbr,
        yearFound: team.yearFound
      };

      //console.log('teamModified', teamModified);
      //console.log('teamData', teamData);
      //console.log('team', team);

      //return ret;

      let req = await fetch('http://xperimental.io:4200/api/v1/add/team', {
        method: 'post',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(teamModified)
      });

      let ret = await req.json();
      //console.log('ret', ret);
      //return ret;

      

      if (ret.success) {
        success++;
        //eslint-disable-next-line
        console.log('Added team: ' + team.abbr + ' to teams DB.');
      }
    }

    return success === teamData.length;
  }
}

var NFL = new NFLAPI();
NFL.init();