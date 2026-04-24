const fs = require('fs');
const path = require('path');

const root = path.resolve(__dirname, '..');
const read = (relative) => fs.readFileSync(path.join(root, relative), 'utf8');
const string = '"((?:\\\\.|[^"\\\\])*)"';
const localised = `LocalisedText\\s*\\{\\s*en:\\s*${string},\\s*ja:\\s*${string},\\s*de:\\s*${string},\\s*fr:\\s*${string},\\s*\\}`;

function decodeRustString(raw) {
    const jsonString = raw
        .replace(/\r/g, '\\r')
        .replace(/\n/g, '\\n')
        .replace(/\t/g, '\\t')
        .replace(/\u2028/g, '\\u2028')
        .replace(/\u2029/g, '\\u2029');
    return JSON.parse(`"${jsonString}"`);
}

function text(en, ja, de, fr) {
    return {
        en: decodeRustString(en),
        ja: decodeRustString(ja),
        de: decodeRustString(de),
        fr: decodeRustString(fr),
    };
}

function parseInfoMap(files, infoType) {
    const out = {};
    const pattern = new RegExp(`(\\d+)\\s*=>\\s*${infoType}\\s*\\{\\s*name:\\s*${localised}`, 'gs');

    for (const file of files) {
        const source = read(file);
        let match;
        while ((match = pattern.exec(source)) !== null) {
            out[match[1]] = text(match[2], match[3], match[4], match[5]);
        }
    }

    return out;
}

function parseTextMap(files) {
    const out = {};
    const pattern = new RegExp(`(\\d+)\\s*=>\\s*${localised}`, 'gs');

    for (const file of files) {
        const source = read(file);
        let match;
        while ((match = pattern.exec(source)) !== null) {
            out[match[1]] = text(match[2], match[3], match[4], match[5]);
        }
    }

    return out;
}

function parseAutoTranslate(file) {
    const out = {};
    const source = read(file);
    const pattern = new RegExp(`\\((\\d+),\\s*(\\d+)\\)\\s*=>\\s*${localised}`, 'gs');
    let match;

    while ((match = pattern.exec(source)) !== null) {
        out[`${match[1]}:${match[2]}`] = text(match[3], match[4], match[5], match[6]);
    }

    return out;
}

const data = {
    duties: parseInfoMap(['src/ffxiv/duties.rs', 'src/ffxiv.rs'], 'DutyInfo'),
    roulettes: parseInfoMap(['src/ffxiv/roulettes.rs', 'src/ffxiv.rs'], 'RouletteInfo'),
    territoryNames: parseTextMap(['src/ffxiv/territory_names.rs']),
    treasureMaps: parseTextMap(['src/ffxiv/treasure_maps.rs']),
    autoTranslate: parseAutoTranslate('src/ffxiv/auto_translate.rs'),
};

const output = `// Auto-generated from src/ffxiv/*.rs. Do not edit by hand.\n(function () {\n    window.RPF_LISTING_DATA = ${JSON.stringify(data)};\n})();\n`;
const outputPath = path.join(root, 'assets/listing-data.js');
fs.writeFileSync(outputPath, output, 'utf8');

console.log(`Wrote ${path.relative(root, outputPath)} (${Buffer.byteLength(output)} bytes)`);
