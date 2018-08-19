const fs = require('fs');
const events = require('events');
const dns = require('dns');
const util = require('util');
const schedule = require('node-schedule');
const Gamedig = require('gamedig');
const McbeRank = require(__basedir + '/public/assets/js/McbeRank-Utils/McbeRank.js');

const mkdir = function(dirPath){
	try{
		fs.mkdirSync(dirPath);
	}catch(err){
		if(err.code !== 'EEXIST') throw err;
	}
}
mkdir("public/");
mkdir("public/data/");
mkdir("public/data/servers/");
mkdir("public/data/statistics/");

/**
 * Define QueryService
 */
function QueryService(){
	this.ping = ping;
	this.query = query;
	this.run = run;
	this.start = start;
}

/**
 * Define Schema
 */
const Server = {
	parse: function(data){
		var address = data.address;

		var schema = {
			address: address,
			online: false,
			last_update: -1,
			last_online: -1,
			hostname: address.host + '-' + address.port,
			version: "Unknown",
			server_engine: "Unknown",
			maxplayers: 0,
			numplayers: 0,
			rank: -1,
			daily_record: {},
			weekly_record: {},
			monthly_record: {},
			players: []
		};

		for(var field in schema){
			schema[field] = data[field] || schema[field];
		}
		return schema;
	},

	simplify: function(data){
		var allowed = [
			'address',
			'online',
			'last_update',
			'last_online',
			'hostname',
			'version',
			'maxplayers',
			'numplayers',
			'rank',
			'daily_record'
		];
		return Object.keys(data)
			.filter(function(key){ return allowed.includes(key); })
			.reduce(function(obj, key){
				return {
					...obj,
					[key]: data[key]
				};
			}, {});
	}
}

/**
 * Called when file error occured
 */
function handleFileError(error){
	if(error) console.log(error);
}

/**
 * Read server data from local storage
 */
function load(address){
	return new Promise(function(resolve){
		fs.readFile(McbeRank.files.servers.server(address), function(error, data){
			var server = Server.parse(error ? { address: address } : JSON.parse(data));

			// update to given address, so we could keep up the address fresh state
			server.address = address;

			resolve(server);
		});
	});
}

/**
 * Get IP from hostname
 */
function lookup(address){
	return new Promise(function(resolve, reject){
		dns.lookup(address.host, function(error, ip){
			if(!error) address.ip = ip;

			resolve(address);
		});
	});
}

/**
 * Send ping
 * 
 * Limited information, but have a high success rate
 */
function ping(address){
	return load(address).then(function(server){
		return new Promise(function(resolve){
			Gamedig.query({
				type: 'minecraftpeping',
				host: address.ip || address.host,
				port: address.port || 19132,
				maxAttempts: 3,
				socketTimeout: 2000
			}).then(function(state){
				server.online = true;
				server.last_online = McbeRank.timestamp();
				server.hostname = state.raw.hostname;
				server.version = state.raw.version;
				server.maxplayers = state.raw.maxplayers;
				server.numplayers = state.raw.numplayers;

				resolve(server);
			}).catch(function(error){
				server.online = false;
				server.numplayers = 0;

				resolve(server);
			});
		}).then(function(server){
			var last_update = McbeRank.fromTimestamp(server.last_update);
			var now = new Date();

			// Update daily record (every AM 4:00)
			var dateInterval = now.getDate() - last_update.getDate();

			//if(dateInterval > 1 || (dateInterval == 1 && now.getHours() >= 4)){
			if(dateInterval > 0){
				server.daily_record.numplayers = server.numplayers;
			}else{
				server.daily_record.numplayers = Math.max(server.daily_record.numplayers, server.numplayers);
			}

			// Update weekly record
			if(last_update.getDay() != now.getDay() && now.getDay() == 1){
				server.weekly_record.numplayers = server.numplayers;
			}else{
				server.weekly_record.numplayers = Math.max(server.weekly_record.numplayers, server.numplayers);
			}

			// Update monthly record
			if(last_update.getMonth() != now.getMonth()){
				server.monthly_record.numplayers = server.numplayers;
			}else{
				server.monthly_record.numplayers = Math.max(server.monthly_record.numplayers, server.numplayers);
			}

			server.last_update = McbeRank.timestamp();

			return server;
		});
	});
}

/**
 * Convert object to string array
 */
function parsePlayers(players){
	return players.map(player => player.name);
}

/**
 * PocketMine-MP format
 */
function parsePlugins(plugins){
	if(plugins.split(": ").length > 1){ // plugins exists?
		return plugins
			.split(": ")[1] // ["PocketMine-MP 1.7 dev", "Plugins...."]
			.split("; ") // ["Plugin", "Plugin" ...]
			.map(plugin => ({ // ["PluginName", "Version"]
				name: plugin.split(" ")[0],
				version: plugin.split(" ")[1] || "undefined"
			}));
	}
	return [];
}

/**
 * Send query (after ping)
 *
 * Contains rich information,
 * but have a low success late (Only to server who has disabled their query)
 */
function query(address){
	return ping(address).then(function(server){
		return new Promise(function(resolve){
			if(!server.online) return resolve(server);

			var address = server.address;

			Gamedig.query({
				type: 'minecraftpe',
				host: address.ip || address.host,
				port: address.port || 19132,
				maxAttempts: 2,
				socketTimeout: 2000
			}).then(function(state){
				if('server_engine' in state.raw){
					server.server_engine = state.raw.server_engine;
				}
				if('players' in state){
					server.players = parsePlayers(state.players);
				}
				if('plugins' in state.raw){
					server.plugins = parsePlugins(state.raw.plugins);
				}

				resolve(server);

			}).catch(function(state){
				resolve(server); // ping successed but query failed
			});
		});
	});
}

function run(){
	console.log("[QueryService] Starting query at " + new Date());

	// minute timestamp
	var time = McbeRank.timestamp();

	var addresses = JSON.parse(fs.readFileSync(McbeRank.files.addresses));

	Promise.all(addresses.map(function(address){

		/**
		 * resolve host
		 */
		return lookup(address);
	})).then(function(addresses){

		/**
		 * Mapping ip => address
		 */
		var addrmap = {};
		for(var address of addresses){
			if(!address.ip) continue;
			
			var identifier = address.ip + ':' + address.port;
			if(!(identifier in addrmap)){
				addrmap[identifier] = address;
				addrmap[identifier].another_hosts = [];
			}else{
				addrmap[identifier].another_hosts.push(address.host);
			}
		}
		addresses = Object.values(addrmap);

		/**
		 * Send query
		 */
		return Promise.all(addresses.map(query));
	}).then(function(results){

		/**
		 * Classify online or offline
		 */
		var online_servers = [];
		var offline_servers = [];

		results.forEach(function(server){
			if(server.online) online_servers.push(server);
			else offline_servers.push(server);
		});

		/**
		 * Sort as players count
		 */
		online_servers.sort(function(a, b){
			if(a.numplayers > b.numplayers) return -1;
			else if(a.numplayers < b.numplayers) return 1;
			else return 0;
		});

		/**
		 * Store rank variable
		 */
		 var rank = 1;
		for(var server of online_servers){
			server.rank = rank++;
		}
		for(var server of offline_servers){
			server.rank = -1;
		}

		/**
		 * Process plugins
		 */
		var plugins_map = {};
		online_servers.forEach(function(server){
			if('plugins' in server){
				server.plugins.forEach(function(plugin){
					var plugin_data = plugins_map[plugin.name];
					if(!plugin_data){
						plugins_map[plugin.name] = plugin_data = {
							plugin: plugin.name,
							servers: 0,
							versions: {}
						};
					}
					plugin_data.servers += 1;
					if(!(plugin.version in plugin_data.versions)){
						plugin_data.versions[plugin.version] = 0;
					}
					plugin_data.versions[plugin.version] += 1;
				});
				delete server['plugins'];
			}
		});

		var plugins = Object.values(plugins_map);
		plugins.sort(function(a, b){
			if(a.servers > b.servers) return -1;
			else if(a.servers < b.servers) return 1;
			else return 0;
		});
		for(var plugin of plugins){
			var ordered = [];
			Object.keys(plugin.versions).sort().reverse().forEach(function(version){
				ordered.push({
					version: version,
					servers: plugin.versions[version]
				});
			});
			plugin.versions = ordered;
		}

		fs.writeFile(McbeRank.files.plugins, JSON.stringify(plugins), handleFileError);

		/**
		 * Record each statistics file
		 */
		online_servers.forEach(function(server){
			var file = McbeRank.files.statistics.server(server.address);
			if(!fs.existsSync(file)){
				fs.writeFileSync(file, "time,numplayers", handleFileError);
			}
			fs.appendFile(file, '\n' + time + ',' + server.numplayers, handleFileError);

			fs.writeFile(McbeRank.files.servers.server(server.address), JSON.stringify(server), handleFileError);
		});

		fs.writeFile(McbeRank.files.online_servers, JSON.stringify(online_servers.map(Server.simplify)), handleFileError);
		fs.writeFile(McbeRank.files.offline_servers, JSON.stringify(offline_servers.map(Server.simplify)), handleFileError);

		/**
		 * Process total
		 */
		var total = {};

		total.numplayers = online_servers.reduce(function(total, current){ return total + current.numplayers; }, 0);
		total.servers = online_servers.length + offline_servers.length;
		total.online_servers = online_servers.length;
		total.plugins = Object.keys(plugins_map).length;

		fs.writeFile(McbeRank.files.total, JSON.stringify(total), handleFileError);
		if(!fs.existsSync(McbeRank.files.statistics.total)){
			fs.writeFileSync(McbeRank.files.statistics.total, "time,numplayers,online_servers");
		}
		fs.appendFile(McbeRank.files.statistics.total, "\n" + time + "," + total.numplayers + "," + total.online_servers, handleFileError);

		console.log("[QueryService] Finished query at " + new Date());
	});
}

function start(scheduleRule){
	console.log("[QueryService] Starting QueryService ...");
	schedule.scheduleJob(scheduleRule || '*/1 * * * *', function(){
		run();
	});
}

module.exports = new QueryService();