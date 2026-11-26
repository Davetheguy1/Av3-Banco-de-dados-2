'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { spawn } = require('node:child_process');
const { pipeline } = require('node:stream/promises');
const { Client } = require('pg');

const EXPECTED_HEADERS = [
  'Id',
  'Nome',
  'DataNascimento',
  'CPF',
  'UF',
  'CEP',
  'Bairro',
  'Municipio',
  'Telefone',
  'RG',
  'Email',
];

const STATE_NAMES = {
  AC: 'Acre',
  AL: 'Alagoas',
  AP: 'Amapa',
  AM: 'Amazonas',
  BA: 'Bahia',
  CE: 'Ceara',
  DF: 'Distrito Federal',
  ES: 'Espirito Santo',
  GO: 'Goias',
  MA: 'Maranhao',
  MT: 'Mato Grosso',
  MS: 'Mato Grosso do Sul',
  MG: 'Minas Gerais',
  PA: 'Para',
  PB: 'Paraiba',
  PR: 'Parana',
  PE: 'Pernambuco',
  PI: 'Piaui',
  RJ: 'Rio de Janeiro',
  RN: 'Rio Grande do Norte',
  RS: 'Rio Grande do Sul',
  RO: 'Rondonia',
  RR: 'Roraima',
  SC: 'Santa Catarina',
  SP: 'Sao Paulo',
  SE: 'Sergipe',
  TO: 'Tocantins',
};

function csvRecords(text) {
  const rows = [];
  let row = [];
  let field = '';
  let quoted = false;

  for (let i = 0; i < text.length; i += 1) {
    const char = text[i];

    if (quoted) {
      if (char === '"' && text[i + 1] === '"') {
        field += '"';
        i += 1;
      } else if (char === '"') {
        quoted = false;
      } else {
        field += char;
      }
    } else if (char === '"') {
      quoted = true;
    } else if (char === ',') {
      row.push(field);
      field = '';
    } else if (char === '\n' || char === '\r') {
      if (char === '\r' && text[i + 1] === '\n') i += 1;
      row.push(field);
      if (row.some((value) => value !== '')) rows.push(row);
      row = [];
      field = '';
    } else {
      field += char;
    }
  }

  if (quoted) throw new Error('O CSV contém um campo com aspas não fechadas.');
  row.push(field);
  if (row.some((value) => value !== '')) rows.push(row);
  return rows;
}

function parseCsv(text) {
  const rows = csvRecords(text.replace(/^\uFEFF/, ''));
  if (rows.length === 0) throw new Error('O CSV está vazio.');

  const headers = rows[0];
  if (headers.length !== EXPECTED_HEADERS.length ||
      EXPECTED_HEADERS.some((header, index) => headers[index] !== header)) {
    throw new Error(`Cabeçalho inesperado. Esperado: ${EXPECTED_HEADERS.join(',')}`);
  }

  return rows.slice(1).map((values, rowIndex) => {
    if (values.length !== headers.length) {
      throw new Error(`Linha ${rowIndex + 2}: esperadas ${headers.length} colunas, recebidas ${values.length}.`);
    }
    return Object.fromEntries(headers.map((header, index) => [header, values[index]]));
  });
}

function normalizeStateKey(value) {
  return value
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toUpperCase()
    .replace(/[^A-Z]/g, '');
}

const STATE_CODES = new Map();
for (const [code, name] of Object.entries(STATE_NAMES)) {
  STATE_CODES.set(normalizeStateKey(code), code);
  STATE_CODES.set(normalizeStateKey(name), code);
}
for (const alias of ['S Catarina', 'Sta Catarina', 'Sta. Catarina']) {
  STATE_CODES.set(normalizeStateKey(alias), 'SC');
}

function normalizeUf(value, lineNumber) {
  const key = normalizeStateKey(value);
  const code = STATE_CODES.get(key);
  if (!code) throw new Error(`Linha ${lineNumber}: UF não reconhecida: "${value}".`);
  return code;
}

function normalizeDate(value, lineNumber) {
  let day;
  let month;
  let year;
  let match = value.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (match) {
    [, year, month, day] = match;
  } else {
    match = value.match(/^(\d{2})[-/](\d{2})[-/](\d{4})$/);
    if (match) [, day, month, year] = match;
  }

  if (!day) throw new Error(`Linha ${lineNumber}: data inválida: "${value}".`);
  const iso = `${year}-${month}-${day}`;
  const date = new Date(`${iso}T00:00:00.000Z`);
  if (Number.isNaN(date.getTime()) || date.toISOString().slice(0, 10) !== iso) {
    throw new Error(`Linha ${lineNumber}: data inválida: "${value}".`);
  }
  return `${day}-${month}-${year}`;
}

function digitsOnly(value) {
  return value.replace(/\D/g, '');
}

function normalizeRow(row, lineNumber) {
  return {
    Id: row.Id,
    Nome: row.Nome.toLocaleLowerCase('pt-BR'),
    DataNascimento: normalizeDate(row.DataNascimento, lineNumber),
    CPF: digitsOnly(row.CPF),
    UF: normalizeUf(row.UF, lineNumber),
    CEP: digitsOnly(row.CEP),
    Bairro: row.Bairro.toLocaleLowerCase('pt-BR'),
    Municipio: row.Municipio.toLocaleLowerCase('pt-BR'),
    Telefone: digitsOnly(row.Telefone),
    RG: digitsOnly(row.RG),
    Email: row.Email,
  };
}

async function createSqlSnapshot(databaseConfig) {
  const container = process.env.PG_DOCKER_CONTAINER || 'bd2-av3-postgres';
  const snapshotPath = path.join(__dirname, 'backup-bd2_av3.sql');
  const temporaryPath = `${snapshotPath}.tmp`;
  const dumpProcess = spawn('docker', [
    'exec',
    container,
    'pg_dump',
    '--username', databaseConfig.user,
    '--dbname', databaseConfig.database,
    '--inserts',
  ], { windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
  let stderr = '';
  dumpProcess.stderr.setEncoding('utf8');
  dumpProcess.stderr.on('data', (chunk) => {
    if (stderr.length < 12000) stderr += chunk;
  });

  try {
    const exitCodePromise = new Promise((resolve, reject) => {
      dumpProcess.once('error', reject);
      dumpProcess.once('close', resolve);
    });
    const pipePromise = pipeline(
      dumpProcess.stdout,
      fs.createWriteStream(temporaryPath, { encoding: 'utf8' }),
    );
    const [exitCode] = await Promise.all([exitCodePromise, pipePromise]);
    if (exitCode !== 0) {
      throw new Error(`pg_dump falhou: ${stderr.trim() || `código ${exitCode}`}`);
    }

    if (fs.existsSync(snapshotPath)) fs.rmSync(snapshotPath);
    fs.renameSync(temporaryPath, snapshotPath);
    return snapshotPath;
  } catch (error) {
    if (fs.existsSync(temporaryPath)) fs.rmSync(temporaryPath);
    throw error;
  }
}

async function main() {
  const csvPath = path.resolve(process.argv[2] || path.join(__dirname, 'modelo.csv'));
  const input = fs.readFileSync(csvPath, 'utf8');
  const rows = parseCsv(input).map((row, index) => normalizeRow(row, index + 2));

  const databaseConfig = {
    host: process.env.PGHOST || '127.0.0.1',
    port: Number(process.env.PGPORT || 15432),
    user: process.env.PGUSER || 'postgres',
    password: process.env.PGPASSWORD || 'Av3Grupo5Db2026',
    database: process.env.PGDATABASE || 'bd2_av3',
  };
  const client = new Client(databaseConfig);

  const insertSql = `
    INSERT INTO "Pessoas" (
      "Id", "Nome", "DataNascimento", "CPF", "UF", "CEP",
      "Bairro", "Municipio", "Telefone", "RG", "Email"
    ) VALUES (
      $1, $2, TO_DATE($3, 'DD-MM-YYYY'), $4, $5, $6,
      $7, $8, $9, $10, $11
    )
    ON CONFLICT ("Id") DO UPDATE SET
      "Nome" = EXCLUDED."Nome",
      "DataNascimento" = EXCLUDED."DataNascimento",
      "CPF" = EXCLUDED."CPF",
      "UF" = EXCLUDED."UF",
      "CEP" = EXCLUDED."CEP",
      "Bairro" = EXCLUDED."Bairro",
      "Municipio" = EXCLUDED."Municipio",
      "Telefone" = EXCLUDED."Telefone",
      "RG" = EXCLUDED."RG",
      "Email" = EXCLUDED."Email"`;

  try {
    await client.connect();
    await client.query('BEGIN');
    await client.query('ALTER TABLE "Pessoas" ALTER COLUMN "CPF" TYPE VARCHAR(11) USING RTRIM("CPF")');
    await client.query('ALTER TABLE "Pessoas" ALTER COLUMN "CEP" TYPE VARCHAR(8) USING RTRIM("CEP")');

    for (const row of rows) {
      await client.query(insertSql, EXPECTED_HEADERS.map((header) => row[header]));
    }

    await client.query('COMMIT');
    const snapshotPath = await createSqlSnapshot(databaseConfig);
    console.log(`${rows.length} registro(s) normalizado(s) e gravado(s) em "Pessoas".`);
    console.log(`Dump SQL atualizado: ${snapshotPath}`);
  } catch (error) {
    try {
      await client.query('ROLLBACK');
    } catch {
      // A conexão pode já estar encerrada.
    }
    throw error;
  } finally {
    await client.end();
  }
}

main().catch((error) => {
  console.error(`Erro: ${error.message}`);
  process.exitCode = 1;
});
