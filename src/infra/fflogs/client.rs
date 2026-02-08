//! FFLogs 관련 유틸리티
//!
//! 서버 이름에서 FFLogs 리전을 추출하는 기능을 제공합니다.

/// 서버 이름에서 리전 추출
pub fn get_region_from_server(server: &str) -> &'static str {
    // JP (Elemental, Gaia, Mana, Meteor)
    let jp_servers = [
        "Aegis", "Atomos", "Carbuncle", "Garuda", "Gungnir", "Kujata", "Tonberry", "Typhon",
        "Alexander", "Bahamut", "Durandal", "Fenrir", "Ifrit", "Ridill", "Tiamat", "Ultima",
        "Anima", "Asura", "Chocobo", "Hades", "Ixion", "Masamune", "Pandaemonium", "Titan",
        "Belias", "Mandragora", "Ramuh", "Shinryu", "Unicorn", "Valefor", "Yojimbo", "Zeromus"
    ];

    // NA (Aether, Primal, Crystal, Dynamis)
    let na_servers = [
        // Aether
        "Adamantoise", "Cactuar", "Faerie", "Gilgamesh", "Jenova", "Midgardsormr", "Sargatanas", "Siren",
        // Primal
        "Behemoth", "Excalibur", "Exodus", "Famfrit", "Hyperion", "Lamia", "Leviathan", "Ultros",
        // Crystal
        "Balmung", "Brynhildr", "Coeurl", "Diabolos", "Goblin", "Malboro", "Mateus", "Zalera",
        // Dynamis
        "Halicarnassus", "Maduin", "Marilith", "Seraph", "Cuchulainn", "Golem", "Kraken", "Rafflesia",
    ];

    // EU (Chaos, Light)
    let eu_servers = [
        // Chaos
        "Cerberus", "Louisoix", "Moogle", "Omega", "Phantom", "Ragnarok", "Sagittarius", "Spriggan",
        // Light
        "Alpha", "Lich", "Odin", "Phoenix", "Raiden", "Shiva", "Twintania", "Zodiark",
    ];

    // OCE (Materia)
    let oce_servers = ["Bismarck", "Ravana", "Sephirot", "Sophia", "Zurvan"];

    // Normalize input
    let s = server.trim();

    if jp_servers.iter().any(|name| name.eq_ignore_ascii_case(s)) {
        "JP"
    } else if na_servers.iter().any(|name| name.eq_ignore_ascii_case(s)) {
        "NA"
    } else if eu_servers.iter().any(|name| name.eq_ignore_ascii_case(s)) {
        "EU"
    } else if oce_servers.iter().any(|name| name.eq_ignore_ascii_case(s)) {
        "OC" 
    } else {
        "NA" // Default fallback
    }
}
