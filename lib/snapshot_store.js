const fs = require('fs/promises');
const path = require('path');

const OUTPUT_DIR = path.join(process.cwd(), 'data', 'output');

function snapshotPath(date) {
  const clean = String(date).replace(/-/g, '');
  return path.join(OUTPUT_DIR, `${clean}.json`);
}

async function readJson(filePath) {
  const text = await fs.readFile(filePath, 'utf8');
  return JSON.parse(text);
}

async function listSnapshotDates() {
  try {
    const entries = await fs.readdir(OUTPUT_DIR);
    return entries
      .filter((name) => /^\d{8}\.json$/.test(name))
      .map((name) => name.slice(0, 8))
      .sort()
      .reverse();
  } catch (error) {
    if (error && error.code === 'ENOENT') {
      return [];
    }
    throw error;
  }
}

async function readSnapshotByDate(date) {
  const clean = String(date).replace(/-/g, '');
  if (!/^\d{8}$/.test(clean)) {
    throw new Error('date must be YYYYMMDD or YYYY-MM-DD');
  }

  const filePath = snapshotPath(clean);
  return await readJson(filePath);
}

async function readLatestSnapshot() {
  const latestPath = path.join(OUTPUT_DIR, 'latest.json');
  try {
    return await readJson(latestPath);
  } catch (error) {
    if (!error || error.code !== 'ENOENT') {
      throw error;
    }
  }

  const dates = await listSnapshotDates();
  if (!dates.length) {
    throw new Error('No snapshot data available');
  }
  return await readSnapshotByDate(dates[0]);
}

module.exports = {
  OUTPUT_DIR,
  listSnapshotDates,
  readLatestSnapshot,
  readSnapshotByDate,
};
