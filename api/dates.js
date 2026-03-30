const { list_snapshot_dates } = require('../lib/snapshot_store');

module.exports = async function handler(req, res) {
  if (req.method && req.method !== 'GET') {
    res.setHeader('Allow', 'GET');
    return res.status(405).json({ detail: 'Method not allowed' });
  }

  try {
    const dates = await list_snapshot_dates();
    res.setHeader('Cache-Control', 'no-store');
    return res.status(200).json({ dates });
  } catch (error) {
    return res.status(500).json({ detail: error.message || 'Failed to list dates' });
  }
};
