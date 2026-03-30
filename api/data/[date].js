const { read_snapshot_by_date } = require('../../lib/snapshot_store');

module.exports = async function handler(req, res) {
  if (req.method && req.method !== 'GET') {
    res.setHeader('Allow', 'GET');
    return res.status(405).json({ detail: 'Method not allowed' });
  }

  try {
    const { date } = req.query || {};
    const payload = await read_snapshot_by_date(String(date || ''));
    res.setHeader('Cache-Control', 'no-store');
    return res.status(200).json(payload);
  } catch (error) {
    const status = String(error.message || '').includes('date must be') ? 400 : 404;
    return res.status(status).json({ detail: error.message || 'Snapshot not found' });
  }
};
