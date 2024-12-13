use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use serde::{Deserialize, Serialize};
use crate::broker::app_broker::BrokerConfig;
use crate::error::raderr;
use crate::utils::utils::TokenType::Name;
use crate::utils::utils::Validation;

#[derive(Serialize, Deserialize, Clone)]
pub struct ConfigRaw {
    pub broker: BrokerConfig,
    pub apps: Vec<String>,
    pub routing: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct RouteSrc {
    pub instance_id: String,
    pub msg_types: Vec<String>,
    pub routing_keys: Vec<String>,
}

impl RouteSrc {
    pub fn new(instance_id: String, msg_types: Vec<String>, routing_keys: Vec<String>) -> Self {
        RouteSrc {
            instance_id,
            msg_types,
            routing_keys,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Route {
    pub src: RouteSrc,
    pub dst: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct App {
    pub name: String,
    pub version: String,

    ///this is a combination of the name and the unique instance id eg.
    /// if the name is "echo" and the instance id was 0, then this field
    /// will be "echo-0"
    pub instance_id: String,
}

impl App {
    pub fn new(name: String, version: String, instance: String) -> Self {
        App {
            name: name.clone(),
            version,
            instance_id: format!("{}-{}", name, instance),
        }
    }
}

pub const VERSION_DEFAULT: &str = "latest";
pub const INSTANCE_DEFAULT: &str = "0";

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    pub broker: BrokerConfig,
    pub apps: Vec<App>,
    pub routes: Vec<Route>,
}

impl Config {
    fn process_instance_string(instance: &str) -> Result<Vec<String>, Box<dyn Error + Sync + Send>> {
        let mut map: HashMap<String, String> = HashMap::new();
        let split = instance.split(",").collect::<Vec<&str>>();
        for s in split {
            if !Validation::is_valid_instance_suffix(s) {
                return raderr(format!("Invalid instance suffix detected in instance string: {}", instance));
            }

            if map.contains_key(s) {
                return raderr(format!("Duplicate instance id found in instance string: {}", instance));
            }
            map.insert(s.to_string(), s.to_string());
        }

        let res = map.keys().cloned().collect::<Vec<String>>();
        Ok(res)
    }

    fn get_version_from_token(token: &str) -> Result<String, Box<dyn Error + Sync + Send>> {
        if !token.starts_with("(") || !token.ends_with(")") || token.len() < 3 {
            return raderr(format!("Token string in invalid format. Expected (xxxxx), got {}", token));
        }

        /* we have version */
        let version = token[1..token.len() - 1].to_string();
        Ok(version)
    }

    fn instance_id_exists(instance_id: &str, apps: &[App]) -> bool {
        apps.iter().any(|a| a.instance_id == instance_id)
    }

    fn process_msg_types(msg_types: &str) -> Result<Vec<String>, Box<dyn Error + Sync + Send>> {
        if !msg_types.starts_with('(') || !msg_types.ends_with(')') {
            return raderr(format!("Invalid message types format for: {}", msg_types));
        }

        let mts = &msg_types[1..msg_types.len() - 1];
        let types = Validation::get_tokens_from_list(mts, Name)?;
        Ok(types)
    }

    fn process_route_src(src: &str, apps: &[App]) -> Result<RouteSrc, Box<dyn Error + Sync + Send>> {
        let mut split = src.split(&[' ', '\t'][..]).collect::<Vec<&str>>();
        if split.is_empty() {
            return raderr(format!("No route src found in source: {}", src));
        }

        //if the item contains spaces, then split will have "" as the last item
        let last = split[split.len() - 1];
        if last.is_empty() {
            split.pop();
        }

        if !Self::instance_id_exists(split[0], apps) {
            return raderr(format!("Invalid route src found in source. Instance ID ({}) not found. Route: {}", split[0], src));
        }

        if split.len() == 1 {
            /* we have only instance id */
            Ok(RouteSrc::new(
                split[0].to_string(),
                vec![],
                vec![]))
        } else if split.len() == 2 {
            /* we have either message types or routing keys */
            if split[1].starts_with('(') {
                /* we have message types */
                let msg_types = Self::process_msg_types(&split[1])?;
                Ok(RouteSrc::new(
                    split[0].to_string(),
                    msg_types,
                    vec![],
                ))
            } else {
                /* we have routing keys */
                let routing_keys = Validation::get_tokens_from_list(&split[1], Name)?;
                Ok(RouteSrc::new(
                    split[0].to_string(),
                    vec![],
                    routing_keys,
                ))
            }
        } else if split.len() == 3 {
            /* we have msg_types and routing keys */
            let msg_types = Self::process_msg_types(&split[1])?;
            let routing_keys = Validation::get_tokens_from_list(&split[2], Name)?;
            Ok(RouteSrc::new(
                split[0].to_string(),
                msg_types,
                routing_keys,
            ))
        } else {
            raderr("Invalid format when processing routing src")
        }
    }

    ///processes a routing item from the config file.
    /// Routing items look like this:
    /// `echo-0 (string, MyRecord) key1,key2 -> kafka-1,rabbit-1`
    ///
    /// The format is:
    /// `src_app_instance_id (message_types) routing_keys -> dst_app_instance_ids`
    /// where:
    /// - src_app_instance_id is the source application instance id (from the apps section)
    /// - message types are optional, and if specified must be a comma
    ///   separated message type strings in round brackets. If not specified,
    ///   defaults to [] which means allow all message types
    /// - routing_keys are optional, and if specified must be a comma
    ///   separated list of routing keys. If not specified, defaults to []
    ///   which means allow any/all routing keys
    /// - dst_app_instance_ids must be a comma separated list of destination app
    ///   instance ids, as specified in the apps section
    fn process_routing_item(routing_raw: &str, apps: &[App]) -> Result<Vec<Route>, Box<dyn Error + Sync + Send>> {
        let split = routing_raw.split("->").collect::<Vec<&str>>();
        if split.len() != 2 {
            return raderr(format!("Invalid routing item format for {}", routing_raw));
        }

        let src = split[0].trim();
        let dst = split[1].trim();
        let src = Config::process_route_src(src, apps)?;
        let dsts = Validation::get_tokens_from_list(dst, Name)?;

        let mut ret = vec![];
        for dst in &dsts {
            if !Self::instance_id_exists(dst, apps) {
                return raderr(format!("Invalid dst specified. Not found in apps list. dst=={}", dst));
            }

            ret.push(Route {
                src: src.clone(),
                dst: dst.to_string(),
            })
        }

        Ok(ret)
    }


    ///processes an app item from the config file.
    /// App items look like this:
    /// `app (0.0.1) 0,1`
    ///
    /// The format is:
    /// `app_name (version) instance_ids`
    /// where:
    /// app_name - alphanumeric and underscores allowed
    /// version - must be specified in round brackets. alphanumeric, underscores, dashes, and dots are allowed. If not specified, defaults to "latest"
    /// instance_ids - comma separated list of instance IDs.
    ///                Optional. If not specified, defaults to "0".
    ///                alphanumeric, underscores, dashes, and dots allowed.
    ///                Each instance id is appended to the name eg.
    ///                if the app name is "echo" and the instance IDs are 0,test, then
    ///                the app names are really echo-0, and echo-test.
    ///                The combination of app name and instance id must be unique
    fn process_app_item(app_raw: &str) -> Result<Vec<App>, Box<dyn Error + Sync + Send>> {
        let split = app_raw.split(&[' ', '\t']).collect::<Vec<&str>>();
        if split.is_empty() || split.len() > 3 {
            return raderr(format!("Invalid app item format for {}", app_raw));
        }

        let name = split[0];
        if !Validation::is_valid_variable(name) {
            return raderr(format!("Invalid app name for {} in line {}", name, app_raw));
        }

        if split.len() == 1 {
            let version = VERSION_DEFAULT.to_string();
            let instance = INSTANCE_DEFAULT.to_string();
            return Ok(vec![App::new(name.to_string(),
                                    version,
                                    instance)]);
        }

        if split.len() == 2 {
            //we have version or instance(s)
            if split[1].starts_with("(") {
                /* we have version */
                let version = Self::get_version_from_token(split[1])?;
                if !Validation::is_valid_version(&version) {
                    return raderr(format!("Invalid version format for {} in line {}", split[1], app_raw));
                }

                Ok(vec![App::new(name.to_string(),
                                 version,
                                 INSTANCE_DEFAULT.to_string())])
            } else {
                /* we have instances only */
                let instances = Self::process_instance_string(split[1])?;
                let mut apps = vec![];
                for i in &instances {
                    apps.push(App::new(
                        name.to_string(),
                        VERSION_DEFAULT.to_string(),
                        i.to_string(),
                    ));
                }
                Ok(apps)
            }
        } else {
            //we have version and instance(s)
            let version = Self::get_version_from_token(split[1])?;
            let instances = Self::process_instance_string(split[2])?;

            let mut apps = vec![];
            for i in &instances {
                apps.push(App::new(
                    name.to_string(),
                    version.to_string(),
                    i.to_string()));
            }
            Ok(apps)
        }
    }

    pub fn new_from(raw: &ConfigRaw) -> Result<Self, Box<dyn Error + Sync + Send>> {
        //the full list of apps objects
        let mut apps = vec![];

        let mut app_map = HashMap::<String, String>::new();
        for app_str in &raw.apps {
            let mut app = Self::process_app_item(app_str)?;
            while !&app.is_empty() {
                let a = app.remove(0);
                let instance_id = a.instance_id.clone();

                //check for duplicates between apps
                if app_map.contains_key(a.instance_id.as_str()) {
                    return raderr(format!("Duplicate app instance ID found: {}", a.instance_id));
                }
                app_map.insert(instance_id.clone(), instance_id);

                apps.push(a);
            }
        }

        let mut routes = vec![];
        for raw_route in &raw.routing {
            let mut item_routes = Self::process_routing_item(raw_route, &apps)?;
            while !item_routes.is_empty() {
                let r = item_routes.remove(0);
                routes.push(r);
            }
        }

        Ok(Config {
            broker: raw.broker.clone(),
            apps,
            routes,
        })
    }

    pub fn load(path: &str) -> Result<Config, Box<dyn Error + Sync + Send>> {
        let f = File::open(path)?;
        let config_raw = serde_yaml::from_reader::<&File, ConfigRaw>(&f)?;
        Config::new_from(&config_raw)
    }
}

